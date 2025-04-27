#include <cstdint>
#include <list>
#include <map>
#include <cmath>
#include <set>
#include <vector>
#include <string>
#include <algorithm>
#include <queue>

#include <batprotocol.hpp>
#include <intervalset.hpp>

#include "batsim_edc.h"

using namespace batprotocol;

// This variable is needed by analyze.py - DO NOT CHANGE THIS LINE FORMAT
double pourcentage_budget = 1.0;

struct SchedJob {
  std::string job_id;
  uint8_t nb_hosts;
  double walltime;              
  double estimated_energy;      
  std::set<int> allocated_hosts;
  double submission_time;       
  double start_time;            
  double expected_end_time;     
};

// Compare jobs by weighted priority (waiting time / energy need)
struct JobPriorityCompare {
  double current_time;
  JobPriorityCompare(double time) : current_time(time) {}
  
  bool operator()(const SchedJob* a, const SchedJob* b) const {
    double wait_a = current_time - a->submission_time;
    double wait_b = current_time - b->submission_time;
    
    // Calculate priority based on waiting time and inverse of energy need
    double priority_a = wait_a / a->estimated_energy;
    double priority_b = wait_b / b->estimated_energy;
    
    return priority_a < priority_b; // Higher priority first
  }
};

MessageBuilder * mb = nullptr;
bool format_binary = true;
std::list<SchedJob*> * jobs = nullptr;
std::map<std::string, SchedJob*> * running_jobs = nullptr;
uint32_t platform_nb_hosts = 0;
std::vector<bool> * host_used = nullptr; // Tracks which hosts are in use

// Energy budget parameters
bool energy_budget_active = true;  
double budget_start_time = 0;      
double budget_end_time = 600;       // seconds
double total_energy_budget = 0;    // Will be calculated once we know platform_nb_hosts
double energy_rate = 0;            // Base rate of energy made available (J/s)
double available_energy = 0;       // Counter for available energy (C_ea)
double last_update_time = 0;       // Time of last energy update
double consumed_energy = 0;        // Total energy consumed so far

// Parameters for energy consumption
double P_comp = 203.12;  // Power consumption of a computing processor (W)
double P_idle = 100.00;   // Power consumption of an idle processor (W)
double P_comp_est = 203.12; // Estimated power for a computing processor (W)
double P_idle_est = 100.00; // Estimated power for an idle processor (W)

// For reservations in reducePC
double reduced_energy_rate = 0; // Reduced rate when there are reservations
double reservation_end_time = 0; // When the current reservation ends
bool has_active_reservation = false; // Flag to track if we have an active reservation

// Forward declarations
double estimate_job_energy(const SchedJob* job);
double estimate_job_power(const SchedJob* job);
double estimate_cluster_power(int computing_hosts, int idle_hosts);
void update_available_energy(double current_time);
bool has_enough_energy(const SchedJob* job, double current_time);
void reserve_energy_reducePC(const SchedJob* job, double start_time, double current_time);
bool allocate_hosts_for_job(SchedJob* job);
bool try_schedule_jobs(double current_time);
SchedJob* find_smallest_job();
SchedJob* find_best_job_for_backfill(double current_time, double available_slots);

// this function is called by batsim to initialize your decision code
uint8_t batsim_edc_init(const uint8_t * data, uint32_t size, uint32_t flags) {
  format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
  if ((flags & (BATSIM_EDC_FORMAT_BINARY | BATSIM_EDC_FORMAT_JSON)) != flags) {
    printf("Unknown flags used, cannot initialize myself.\n");
    return 1;
  }

  mb = new MessageBuilder(!format_binary);
  jobs = new std::list<SchedJob*>();
  running_jobs = new std::map<std::string, SchedJob*>();
  host_used = new std::vector<bool>();

  // ignore initialization data
  (void) data;
  (void) size;

  return 0;
}

// this function is called by batsim to deinitialize your decision code
uint8_t batsim_edc_deinit() {
  delete mb;
  mb = nullptr;

  if (jobs != nullptr) {
    for (auto * job : *jobs) {
      delete job;
    }
    delete jobs;
    jobs = nullptr;
  }

  if (running_jobs != nullptr) {
    // Note: The jobs are already deleted in the jobs list cleanup
    delete running_jobs;
    running_jobs = nullptr;
  }
  
  if (host_used != nullptr) {
    delete host_used;
    host_used = nullptr;
  }

  return 0;
}

// Find the job with the smallest energy requirement
SchedJob* find_smallest_job() {
  if (jobs->empty()) return nullptr;
  
  SchedJob* smallest = jobs->front();
  double smallest_energy = estimate_job_energy(smallest);
  
  for (auto job : *jobs) {
    double job_energy = estimate_job_energy(job);
    if (job_energy < smallest_energy) {
      smallest = job;
      smallest_energy = job_energy;
    }
  }
  
  return smallest;
}

// Find the best job for backfilling based on a weighted priority
SchedJob* find_best_job_for_backfill(double current_time, double available_slots) {
  if (jobs->empty()) return nullptr;
  
  JobPriorityCompare comparator(current_time);
  
  std::priority_queue<SchedJob*, std::vector<SchedJob*>, JobPriorityCompare> job_queue{comparator};
  
  for (auto job : *jobs) {
    if (job->nb_hosts <= available_slots) {
      job_queue.push(job);
    }
  }
  
  if (job_queue.empty()) return nullptr;
  
  return job_queue.top();
}

double estimate_job_energy(const SchedJob* job) {
  return job->nb_hosts * P_comp_est * job->walltime;
}

double estimate_job_power(const SchedJob* job) {
  return job->nb_hosts * P_comp_est;
}

double estimate_cluster_power(int computing_hosts, int idle_hosts) {
  return computing_hosts * P_comp_est + idle_hosts * P_idle_est;
}

void update_available_energy(double current_time) {
  if (!energy_budget_active) return;
  
  if (current_time >= budget_start_time && current_time <= budget_end_time) {
    double elapsed = current_time - last_update_time;
    
    if (last_update_time < budget_start_time) {
      elapsed = current_time - budget_start_time;
    }
    
    double period_energy_consumed = 0.0;
    for (const auto& [job_id, job] : *running_jobs) {
      period_energy_consumed += job->nb_hosts * P_comp_est * elapsed;
    }
    
    int running_hosts_count = 0;
    for (size_t i = 0; i < host_used->size(); i++) {
      if ((*host_used)[i]) {
        running_hosts_count++;
      }
    }
    int idle_hosts = platform_nb_hosts - running_hosts_count;
    period_energy_consumed += idle_hosts * P_idle_est * elapsed;
    
    consumed_energy += period_energy_consumed;
    
    if (elapsed > 0) {
      double added_energy = reduced_energy_rate * elapsed;
      available_energy += added_energy;
    }
    
    if (current_time >= reservation_end_time && has_active_reservation) {
      reduced_energy_rate = energy_rate;
      has_active_reservation = false;
    }
  }
  
  last_update_time = current_time;
}

// Checks if there's enough energy to run a job
bool has_enough_energy(const SchedJob* job, double current_time) {
  if (!energy_budget_active || current_time < budget_start_time || current_time > budget_end_time) {
    return true; // No energy constraints outside the budget period
  }
  
  double job_energy = estimate_job_energy(job);
  
  bool has_enough = job_energy <= available_energy;
  
  if (!has_enough && available_energy < (job_energy * 0.01)) {
    printf("Severe energy shortage: job %s needs %.2f J, but only %.2f J available (%.2f%%)\n",
           job->job_id.c_str(), job_energy, available_energy, 
           (available_energy / job_energy) * 100.0);
  }
  
  return has_enough;
}

// Make a reservation for a job's energy (reducePC approach)
void reserve_energy_reducePC(const SchedJob* job, double start_time, double current_time) {
  if (!energy_budget_active || current_time < budget_start_time || current_time > budget_end_time) {
    return; // No energy constraints outside the budget period
  }
  
  double job_energy = estimate_job_energy(job);
  
  double time_until_start = start_time - current_time;
  
  if (time_until_start > 0) {
    double energy_rate_reduction = job_energy / time_until_start;
    
    double min_rate_factor = 0.3; // Default 30%
    double min_rate = energy_rate * min_rate_factor;
    reduced_energy_rate = std::max(min_rate, energy_rate - energy_rate_reduction);
    
    reservation_end_time = start_time;
    has_active_reservation = true;
  }
}

// Find a contiguous set of available hosts for a job
// Returns true if allocation was successful, false otherwise
bool allocate_hosts_for_job(SchedJob* job) {
  if (job->nb_hosts > platform_nb_hosts) {
    return false;
  }
  
  // Find a contiguous range of free hosts
  int consecutive_free = 0;
  int start_host = -1;
  
  for (size_t i = 0; i < host_used->size(); i++) {
    if (!(*host_used)[i]) {
      if (consecutive_free == 0) {
        start_host = i;
      }
      consecutive_free++;
      
      if (consecutive_free == job->nb_hosts) {
        // We found enough consecutive hosts
        for (int j = start_host; j < start_host + job->nb_hosts; j++) {
          (*host_used)[j] = true;
          job->allocated_hosts.insert(j);
        }
        return true;
      }
    } else {
      consecutive_free = 0;
      start_host = -1;
    }
  }
  
  return false; // Couldn't find enough consecutive hosts
}

// Try to schedule jobs from the queue
// Returns true if any job was scheduled, false otherwise
bool try_schedule_jobs(double current_time) {
  // If no jobs, nothing to do
  if (jobs->empty()) return false;
  
  bool any_job_scheduled = false;
  
  // Calculate available hosts
  int available_hosts = 0;
  for (size_t i = 0; i < host_used->size(); i++) {
    if (!(*host_used)[i]) {
      available_hosts++;
    }
  }
  
  SchedJob* first_job = jobs->front();
  
  // Check if we can run the first job now - need both resources and energy
  bool can_run_first_job = false;
  if (first_job->nb_hosts <= available_hosts) {
    // We have enough hosts - now check energy
    if (has_enough_energy(first_job, current_time)) {
      can_run_first_job = true;
    } else {
      // Log energy limitation
      printf("Job %s cannot run due to energy constraints (needs %.2f J, available %.2f J)\n",
             first_job->job_id.c_str(), estimate_job_energy(first_job), available_energy);
    }
  }
  
  // If we can run the first job, do it
  if (can_run_first_job) {
    if (allocate_hosts_for_job(first_job)) {
      // Convert the allocated hosts to interval set for Batsim
      IntervalSet hosts;
      for (int host_id : first_job->allocated_hosts) {
        hosts.insert(host_id);
      }
      
      mb->add_execute_job(first_job->job_id, hosts.to_string_hyphen());
      
      // Set metadata for the job
      first_job->start_time = current_time;
      first_job->expected_end_time = current_time + first_job->walltime;
      
      // Add to running jobs and remove from queue
      (*running_jobs)[first_job->job_id] = first_job;
      jobs->pop_front();
      
      any_job_scheduled = true;
      
      // Cancel any active reservation since we scheduled the first job
      if (has_active_reservation) {
        reduced_energy_rate = energy_rate;
        has_active_reservation = false;
      }
      
      // Update available hosts for backfilling
      available_hosts -= first_job->nb_hosts;
    }
  }
  
  // If we couldn't schedule the first job or we still have resources, try backfilling
  if (!jobs->empty()) {
    SchedJob* reserved_job = nullptr;
    double earliest_start_time = current_time;
    
    // If we couldn't schedule the first job, make a reservation for it
    if (!can_run_first_job && !jobs->empty()) {
      reserved_job = jobs->front();
      
      // Calculate when resources will be available
      if (reserved_job->nb_hosts > available_hosts) {
        if (!running_jobs->empty()) {
          // Use expected end times to make a better estimation
          double earliest_resource_time = current_time + 1000.0; // Large initial value
          
          for (const auto& [job_id, job] : *running_jobs) {
            earliest_resource_time = std::min(earliest_resource_time, job->expected_end_time);
          }
          
          earliest_start_time = earliest_resource_time;
        }
      }
      
      // Calculate when energy will be available
      if (energy_budget_active && current_time >= budget_start_time && current_time <= budget_end_time) {
        double needed_energy = estimate_job_energy(reserved_job);
        double missing_energy = needed_energy - available_energy;
        
        if (missing_energy > 0) {
          // Calculate time to accumulate with the base energy rate
          double time_to_accumulate = missing_energy / energy_rate;
          
          // Add a small buffer
          time_to_accumulate *= 1.1;
          
          double energy_start_time = current_time + time_to_accumulate;
          
          if (energy_start_time > earliest_start_time) {
            earliest_start_time = energy_start_time;
          }
        }
      }
      
      // Make reservation for the first job
      if (earliest_start_time > current_time) {
        reserve_energy_reducePC(reserved_job, earliest_start_time, current_time);
      }
    }
    
    // Try to backfill other jobs
    if (available_hosts > 0) {
      // Create a copy of the jobs list for backfilling
      std::list<SchedJob*> candidates;
      auto reserved_it = jobs->begin();
      
      if (reserved_job != nullptr) {
        // Skip the reserved job
        reserved_it++;
      }
      
      // Add all candidates that could potentially be backfilled
      for (auto it = reserved_it; it != jobs->end(); ++it) {
        SchedJob* job = *it;
        if (job->nb_hosts <= available_hosts) {
          // Check if job finishes before reserved job start
          if (reserved_job == nullptr || 
              (current_time + job->walltime <= earliest_start_time)) {
            candidates.push_back(job);
          }
        }
      }
      
      // Sort candidates by shortest job first
      candidates.sort([](const SchedJob* a, const SchedJob* b) {
        return a->walltime < b->walltime;
      });
      
      // Try to backfill jobs
      for (auto candidate : candidates) {
        // Check if we still have energy for this job
        if (has_enough_energy(candidate, current_time)) {
          if (allocate_hosts_for_job(candidate)) {
            // Convert the allocated hosts to interval set for Batsim
            IntervalSet hosts;
            for (int host_id : candidate->allocated_hosts) {
              hosts.insert(host_id);
            }
            
            mb->add_execute_job(candidate->job_id, hosts.to_string_hyphen());
            
            // Set metadata for the job
            candidate->start_time = current_time;
            candidate->expected_end_time = current_time + candidate->walltime;
            
            // Add to running jobs
            (*running_jobs)[candidate->job_id] = candidate;
            
            // Remove from queue
            auto it = std::find(jobs->begin(), jobs->end(), candidate);
            if (it != jobs->end()) {
              jobs->erase(it);
            }
            
            any_job_scheduled = true;
            
            // Update available hosts
            available_hosts -= candidate->nb_hosts;
            
            // Stop if no more resources
            if (available_hosts <= 0) {
              break;
            }
          }
        } else {
          printf("Cannot backfill job %s due to energy constraints (needs %.2f J, available %.2f J)\n",
                 candidate->job_id.c_str(), estimate_job_energy(candidate), available_energy);
        }
      }
    }
  }
  
  return any_job_scheduled;
}

// this function is called by batsim when it thinks that you may take decisions
uint8_t batsim_edc_take_decisions(
  const uint8_t * what_happened,
  uint32_t what_happened_size,
  uint8_t ** decisions,
  uint32_t * decisions_size)
{
  (void) what_happened_size;

  // deserialize the message received
  auto * parsed = deserialize_message(*mb, !format_binary, what_happened);
  double current_time = parsed->now();
  
  // Update available energy based on the current time
  update_available_energy(current_time);

  // clear data structures to take the next decisions.
  mb->clear(current_time);

  bool should_schedule = false;

  // traverse all events that have just been received
  auto nb_events = parsed->events()->size();
  for (unsigned int i = 0; i < nb_events; ++i) {
    auto event = (*parsed->events())[i];
    printf("reducePC_IDLE received event type='%s'\n", batprotocol::fb::EnumNamesEvent()[event->event_type()]);
    switch (event->event_type()) {
      // protocol handshake
      case fb::Event_BatsimHelloEvent: {
        mb->add_edc_hello("reducePC_IDLE", "1.0.0");
      } break;
      // batsim tells you that the simulation starts, providing you various initialization information
      case fb::Event_SimulationBeginsEvent: {
        auto simu_begins = event->event_as_SimulationBeginsEvent();
        platform_nb_hosts = simu_begins->computation_host_number();
        
        // Initialize the host_used vector
        host_used->resize(platform_nb_hosts, false);
        
        // Recalculate energy budget with the actual number of hosts and percentage
        if (energy_budget_active) {
          double period_duration = budget_end_time - budget_start_time;
          
          // Calculate max energy budget (100%) - what would be used if all processors computing
          double max_energy = platform_nb_hosts * P_comp * period_duration;
          
          // Calculate total energy budget based on the percentage parameter
          total_energy_budget = pourcentage_budget * max_energy;
          energy_rate = total_energy_budget / period_duration;
          reduced_energy_rate = energy_rate;
          
          // Extending budget period to cover the entire simulation (for analysis.py)
          budget_end_time = 1000000.0; // very large value to ensure budget always applies
          
          printf("Energy budget: %.2f%% of max (%.2f joules), rate: %.2f W\n", 
                 pourcentage_budget * 100, total_energy_budget, energy_rate);
        }
        
        last_update_time = current_time;
        should_schedule = true;
      } break;
      // a job has just been submitted
      case fb::Event_JobSubmittedEvent: {
        auto parsed_job = event->event_as_JobSubmittedEvent();
        auto job = new SchedJob();
        job->job_id = parsed_job->job_id()->str();
        job->nb_hosts = parsed_job->job()->resource_request();
        job->walltime = parsed_job->job()->walltime();
        job->estimated_energy = job->nb_hosts * P_comp_est * job->walltime;
        job->submission_time = current_time;
        job->start_time = 0.0;
        job->expected_end_time = 0.0;
        
        if (job->nb_hosts > platform_nb_hosts) {
          mb->add_reject_job(job->job_id);
          delete job;
        }
        else {
          jobs->push_back(job);
          should_schedule = true;
        }
      } break;
      // a job has just completed
      case fb::Event_JobCompletedEvent: {
        auto job_completed = event->event_as_JobCompletedEvent();
        std::string completed_job_id = job_completed->job_id()->str();
        
        // Remove from running jobs and free hosts
        if (running_jobs->find(completed_job_id) != running_jobs->end()) {
          SchedJob* job = (*running_jobs)[completed_job_id];
          
          // Free the hosts used by this job
          for (int host_id : job->allocated_hosts) {
            if (host_id >= 0 && host_id < static_cast<int>(host_used->size())) {
              (*host_used)[host_id] = false;
            }
          }
          
          delete job;
          running_jobs->erase(completed_job_id);
          
          // Flag that we should try to schedule jobs after a job completion
          should_schedule = true;
          
          // Reset energy rate if we had an active reservation
          if (has_active_reservation) {
            reduced_energy_rate = energy_rate;
            has_active_reservation = false;
          }
        }
      } break;
      default: break;
    }
  }

  // Try to schedule jobs if we need to
  if (should_schedule) {
    try_schedule_jobs(current_time);
  }

  // serialize decisions that have been taken into the output parameters of the function
  mb->finish_message(current_time);
  serialize_message(*mb, !format_binary, const_cast<const uint8_t **>(decisions), decisions_size);
  return 0;
}
