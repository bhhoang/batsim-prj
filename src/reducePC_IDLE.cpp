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

struct SchedJob {
  std::string job_id;
  uint8_t nb_hosts;
  double walltime;              // Estimated execution time (in seconds)
  double estimated_energy;      // Estimated energy consumption for this job
  std::set<int> allocated_hosts;// Set of host IDs allocated to this job
  double submission_time;       // When the job was submitted
  double start_time;            // When the job started (0 if not started)
  double expected_end_time;     // When the job is expected to finish
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
bool format_binary = true; // whether flatbuffers binary or json format should be used
std::list<SchedJob*> * jobs = nullptr;
std::map<std::string, SchedJob*> * running_jobs = nullptr;
uint32_t platform_nb_hosts = 0;
std::vector<bool> * host_used = nullptr; // Tracks which hosts are in use

// Energy budget parameters
bool energy_budget_active = true;  // Enable energy budget by default
double budget_start_time = 0;      // Start immediately
double budget_end_time = 30;       // End after 30 seconds (covers typical simulation)
double total_energy_budget = 0;    // Will be calculated once we know platform_nb_hosts
double energy_rate = 0;            // Base rate of energy made available (joules/second)
double available_energy = 0;       // Counter for available energy (C_ea)
double last_update_time = 0;       // Time of last energy update
double consumed_energy = 0;        // Total energy consumed so far

// Deadlock prevention
double last_job_start_time = 0;    // Last time a job was started
bool emergency_mode = false;       // Emergency mode to prevent deadlocks
double emergency_threshold = 10.0; // Increased to 10 seconds (from 5)
int consecutive_scheduling_failures = 0;  // Count consecutive failures to schedule
int max_failures_before_override = 3;    // After this many failures, override energy constraints

// Energy consumption estimation parameters
double P_comp = 190.74;  // Power consumption of a computing processor (W)
double P_idle = 95.00;   // Power consumption of an idle processor (W)
double P_comp_est = 203.12; // Estimated power for a computing processor (W) - overestimated
double P_idle_est = 100.00; // Estimated power for an idle processor (W) - overestimated

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
bool force_schedule_job(SchedJob* job, double current_time);
double calculate_energy_lookahead(double current_time, double horizon);

// this function is called by batsim to initialize your decision code
uint8_t batsim_edc_init(const uint8_t * data, uint32_t size, uint32_t flags) {
  format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
  if ((flags & (BATSIM_EDC_FORMAT_BINARY | BATSIM_EDC_FORMAT_JSON)) != flags) {
    fprintf(stderr, "Unknown flags used, cannot initialize myself.\n");
    return 1;
  }

  mb = new MessageBuilder(!format_binary);
  jobs = new std::list<SchedJob*>();
  running_jobs = new std::map<std::string, SchedJob*>();
  host_used = new std::vector<bool>();

  // Set hardcoded values for platform_nb_hosts - we'll update this when we get the actual value
  platform_nb_hosts = 2;  // Assume 2 machines as a starting point
  
  // Initialize energy budget parameters
  double budget_factor = 1.1;  // Allow 10% over-budgeting to improve throughput
  double period_duration = budget_end_time - budget_start_time;
  
  // Safe guard against division by zero
  if (period_duration <= 0) {
    fprintf(stderr, "Warning: Invalid budget period duration! Setting to 30 seconds.\n");
    budget_start_time = 0;
    budget_end_time = 30;
    period_duration = 30;
  }
  
  // Calculate initial energy budget 
  total_energy_budget = budget_factor * platform_nb_hosts * P_comp * period_duration;
  
  // Initialize energy rate
  energy_rate = total_energy_budget / period_duration;
  reduced_energy_rate = energy_rate;
  
  fprintf(stderr, "Energy budget initialized: %.2f joules over %.2f seconds\n", 
          total_energy_budget, period_duration);
  fprintf(stderr, "Initial energy rate: %.2f W\n", energy_rate);

  // Parse initialization data (if any)
  if (data != nullptr && size > 0) {
    // Real implementation would parse actual initialization parameters 
    // For now, we're using hardcoded values
  }

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
  
  // Create the comparator
  JobPriorityCompare comparator(current_time);
  
  // Create a priority queue with the comparator
  std::priority_queue<SchedJob*, std::vector<SchedJob*>, JobPriorityCompare> job_queue{comparator};
  
  // Add eligible jobs to priority queue
  for (auto job : *jobs) {
    if (job->nb_hosts <= available_slots) {
      job_queue.push(job);
    }
  }
  
  if (job_queue.empty()) return nullptr;
  
  return job_queue.top();
}

// Force schedules a specific job, ignoring energy constraints
bool force_schedule_job(SchedJob* job, double current_time) {
  if (!job) return false;
  
  // Check if we have enough resources
  int available_hosts = 0;
  for (size_t i = 0; i < host_used->size(); i++) {
    if (!(*host_used)[i]) {
      available_hosts++;
    }
  }
  
  if (job->nb_hosts <= available_hosts) {
    if (allocate_hosts_for_job(job)) {
      // Convert the allocated hosts to interval set for Batsim
      IntervalSet hosts;
      for (int host_id : job->allocated_hosts) {
        hosts.insert(host_id);
      }
      
      mb->add_execute_job(job->job_id, hosts.to_string_hyphen());
      
      // Calculate energy that will be consumed (for logging)
      double job_energy = estimate_job_energy(job);
      
      fprintf(stderr, "FORCE SCHEDULING: Job %s (needs %.2f J with only %.2f J available)\n", 
              job->job_id.c_str(), job_energy, available_energy);
      
      // Add metadata to the job
      job->start_time = current_time;
      job->expected_end_time = current_time + job->walltime;
      
      // Note: We now DON'T subtract all the energy upfront
      // The job's energy consumption will be tracked in real-time
      
      // Add to running jobs and remove from queue
      auto it = std::find(jobs->begin(), jobs->end(), job);
      if (it != jobs->end()) {
        jobs->erase(it);
      }
      (*running_jobs)[job->job_id] = job;
      
      // Reset tracking variables
      last_job_start_time = current_time;
      emergency_mode = false;
      consecutive_scheduling_failures = 0;
      
      // Cancel any active reservation
      reduced_energy_rate = energy_rate;
      has_active_reservation = false;
      
      return true;
    }
  }
  
  return false;
}

// Calculate how much energy will become available within a time horizon
// from running jobs that are expected to finish
double calculate_energy_lookahead(double current_time, double horizon) {
  double future_energy = 0.0;
  
  for (const auto& [job_id, job] : *running_jobs) {
    // Check if job will finish within horizon
    if (job->expected_end_time <= current_time + horizon) {
      // Calculate remaining energy that would have been consumed
      double remaining_time = job->expected_end_time - current_time;
      double energy_to_be_freed = job->nb_hosts * P_comp_est * remaining_time;
      future_energy += energy_to_be_freed;
    }
  }
  
  return future_energy;
}

// Updates the available energy based on the current time
void update_available_energy(double current_time) {
  if (!energy_budget_active) return;
  
  // Only update if we're in the budget period
  if (current_time >= budget_start_time && current_time <= budget_end_time) {
    // Calculate time since last update
    double elapsed = current_time - last_update_time;
    
    if (last_update_time < budget_start_time) {
      // First update in the budget period
      elapsed = current_time - budget_start_time;
    }
    
    // Calculate energy consumed by running jobs during this period - GRADUAL TRACKING
    double period_energy_consumed = 0.0;
    for (const auto& [job_id, job] : *running_jobs) {
      period_energy_consumed += job->nb_hosts * P_comp_est * elapsed;
    }
    
    // Add idle machine power consumption
    int running_hosts_count = 0;
    for (size_t i = 0; i < host_used->size(); i++) {
      if ((*host_used)[i]) {
        running_hosts_count++;
      }
    }
    int idle_hosts = platform_nb_hosts - running_hosts_count;
    period_energy_consumed += idle_hosts * P_idle_est * elapsed;
    
    // Track total energy consumed
    consumed_energy += period_energy_consumed;
    
    // Add energy based on the current (possibly reduced) rate
    if (elapsed > 0) {
      // Determine minimum energy rate based on job queue characteristics
      double min_rate_factor = 0.3; // Default 30%
      
      // If we have many small jobs, be more aggressive with energy saving
      if (!jobs->empty()) {
        int small_jobs = 0;
        double avg_energy = 0.0;
        
        for (auto job : *jobs) {
          avg_energy += estimate_job_energy(job);
        }
        avg_energy /= jobs->size();
        
        for (auto job : *jobs) {
          if (estimate_job_energy(job) < avg_energy * 0.5) {
            small_jobs++;
          }
        }
        
        // If more than half are small jobs, adjust minimum rate
        if (small_jobs > static_cast<int>(jobs->size() / 2)) {
          min_rate_factor = 0.5; // More energy for small jobs
          fprintf(stderr, "Many small jobs detected, setting minimum rate to 50%%\n");
        }
      }
      
      // Make sure reduced_energy_rate is at least min_rate_factor of energy_rate
      if (reduced_energy_rate < energy_rate * min_rate_factor) {
        reduced_energy_rate = energy_rate * min_rate_factor;
        fprintf(stderr, "WARNING: Energy rate was too low, setting to minimum: %.2f W\n", reduced_energy_rate);
      }
      
      double added_energy = reduced_energy_rate * elapsed;
      available_energy += added_energy;
      
      fprintf(stderr, "Time: %.2f, Added energy: %.2f J, Consumed: %.2f J, Available: %.2f J (Total consumed: %.2f J)\n", 
              current_time, added_energy, period_energy_consumed, available_energy, consumed_energy);
    }
    
    // Check if we can restore the original rate (if reservation has ended)
    if (current_time >= reservation_end_time && has_active_reservation) {
      reduced_energy_rate = energy_rate;
      has_active_reservation = false;
      fprintf(stderr, "Time: %.2f, Restoring energy rate to %.2f W (reservation ended)\n", 
              current_time, reduced_energy_rate);
    }
    
    // Check if we should enter emergency mode (no jobs started for a while)
    if (!emergency_mode && current_time - last_job_start_time > emergency_threshold && !jobs->empty()) {
      emergency_mode = true;
      fprintf(stderr, "ENTERING EMERGENCY MODE - No jobs started for %.2f seconds\n", 
              current_time - last_job_start_time);
      
      // In emergency mode, we add a significant energy boost
      double boost = energy_rate * 10.0; // 10 seconds worth of energy
      available_energy += boost;
      fprintf(stderr, "Emergency energy boost: %.2f J, now available: %.2f J\n", boost, available_energy);
    }
    
    // Periodic energy boost if we keep failing to schedule jobs
    if (consecutive_scheduling_failures >= max_failures_before_override) {
      double boost = energy_rate * 5.0; // 5 seconds worth of energy
      available_energy += boost;
      fprintf(stderr, "Scheduling failure energy boost: %.2f J, now available: %.2f J\n", boost, available_energy);
      consecutive_scheduling_failures = 0;
    }
  }
  
  last_update_time = current_time;
}

// Estimates the energy that would be consumed by a job
double estimate_job_energy(const SchedJob* job) {
  return job->nb_hosts * P_comp_est * job->walltime;
}

// Estimates the power consumption of a job
double estimate_job_power(const SchedJob* job) {
  return job->nb_hosts * P_comp_est;
}

// Estimates the cluster's current power consumption
double estimate_cluster_power(int computing_hosts, int idle_hosts) {
  return computing_hosts * P_comp_est + idle_hosts * P_idle_est;
}

// Checks if there's enough energy to run a job, considering lookahead
bool has_enough_energy(const SchedJob* job, double current_time) {
  if (!energy_budget_active || current_time < budget_start_time || current_time > budget_end_time) {
    return true; // No energy constraints outside the budget period
  }
  
  // If we've failed to schedule multiple times, become more lenient
  if (consecutive_scheduling_failures >= max_failures_before_override) {
    return true; // Override energy constraints
  }
  
  // Calculate how much energy the job will consume
  double job_energy = estimate_job_energy(job);
  
  // Look ahead to see how much energy will be freed by jobs ending soon
  double lookahead_horizon = std::min(5.0, job->walltime / 2.0);
  double future_energy = calculate_energy_lookahead(current_time, lookahead_horizon);
  
  // Adjusted available energy including lookahead
  double adjusted_available = available_energy + future_energy;
  
  // Debug output
  fprintf(stderr, "Checking energy for job %s: Needs %.2f J, Available: %.2f J (+ %.2f J from lookahead)\n",
          job->job_id.c_str(), job_energy, available_energy, future_energy);
  
  // In emergency mode, be more lenient
  if (emergency_mode) {
    // In emergency mode, allow jobs that need up to 3x the available energy
    return job_energy <= (adjusted_available * 3.0);
  }
  
  // Normal mode: energy check with lookahead
  return job_energy <= adjusted_available;
}

// Make a reservation for a job's energy (reducePC approach)
void reserve_energy_reducePC(const SchedJob* job, double start_time, double current_time) {
  if (!energy_budget_active || current_time < budget_start_time || current_time > budget_end_time) {
    return; // No energy constraints outside the budget period
  }
  
  // Calculate how much energy the job will need
  double job_energy = estimate_job_energy(job);
  
  // In reducePC, we reduce the rate at which energy is made available
  double time_until_start = start_time - current_time;
  
  if (time_until_start > 0) {
    // Reduce the energy rate to account for the reservation
    double energy_rate_reduction = job_energy / time_until_start;
    
    // IMPROVED: Calculate minimum rate based on queue characteristics
    double min_rate_factor = 0.3; // Default 30%
    
    // If many small jobs are waiting, keep more energy available
    int small_jobs = 0;
    for (auto waiting_job : *jobs) {
      if (waiting_job != job && estimate_job_energy(waiting_job) < job_energy * 0.5) {
        small_jobs++;
      }
    }
    
    if (small_jobs > static_cast<int>(jobs->size() / 3)) {
      min_rate_factor = 0.5; // Higher minimum for small jobs
    }
    
    double min_rate = energy_rate * min_rate_factor;
    reduced_energy_rate = std::max(min_rate, energy_rate - energy_rate_reduction);
    
    reservation_end_time = start_time;
    has_active_reservation = true;
    
    fprintf(stderr, "Energy reservation made: %.2f J for job %s, Reducing rate from %.2f to %.2f W until time %.2f\n", 
           job_energy, job->job_id.c_str(), energy_rate, reduced_energy_rate, start_time);
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
  
  // In emergency mode, try to schedule the job with highest priority
  if (emergency_mode) {
    SchedJob* best_job = find_best_job_for_backfill(current_time, platform_nb_hosts);
    if (best_job && force_schedule_job(best_job, current_time)) {
      any_job_scheduled = true;
      return true;
    }
  }
  
  // Proceed with normal scheduling (try FCFS first, then backfill)
  // Try to schedule the first job in the queue (FCFS)
  SchedJob* first_job = jobs->front();
  
  // Calculate available hosts
  int available_hosts = 0;
  for (size_t i = 0; i < host_used->size(); i++) {
    if (!(*host_used)[i]) {
      available_hosts++;
    }
  }
  
  // Check if we can run the first job now
  if (first_job->nb_hosts <= available_hosts && has_enough_energy(first_job, current_time)) {
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
      
      // We no longer subtract all energy upfront - it will be tracked gradually
      fprintf(stderr, "Job %s started, need %.2f J, available: %.2f J\n", 
              first_job->job_id.c_str(), estimate_job_energy(first_job), available_energy);
      
      // Add to running jobs and remove from queue
      (*running_jobs)[first_job->job_id] = first_job;
      jobs->pop_front();
      
      any_job_scheduled = true;
      last_job_start_time = current_time;
      emergency_mode = false;
      consecutive_scheduling_failures = 0;
      
      // Cancel any active reservation since we scheduled the first job
      if (has_active_reservation) {
        reduced_energy_rate = energy_rate;
        has_active_reservation = false;
        fprintf(stderr, "Canceling reservation because first job has been scheduled\n");
      }
    }
  }
  
  // If we couldn't schedule the first job, try to make a reservation
  // and backfill other jobs
  if (!any_job_scheduled) {
    double earliest_start_time = current_time;
    
    // If we don't have enough resources, estimate when they'll be available
    if (first_job->nb_hosts > available_hosts) {
      if (!running_jobs->empty()) {
        // Use expected end times to make a better estimation
        double earliest_resource_time = current_time + 1000.0; // Large initial value
        
        for (const auto& [job_id, job] : *running_jobs) {
          earliest_resource_time = std::min(earliest_resource_time, job->expected_end_time);
        }
        
        earliest_start_time = earliest_resource_time;
        fprintf(stderr, "Not enough resources for job %s, waiting until %.2f\n", 
                first_job->job_id.c_str(), earliest_start_time);
      }
    }
    
    // If we need more energy, estimate when it'll be available
    if (energy_budget_active && current_time >= budget_start_time && current_time <= budget_end_time) {
      double needed_energy = estimate_job_energy(first_job);
      double missing_energy = needed_energy - available_energy;
      
      if (missing_energy > 0) {
        // Consider energy lookahead for more accurate estimation
        double future_energy = calculate_energy_lookahead(current_time, 5.0);
        missing_energy = std::max(0.0, missing_energy - future_energy);
        
        // Calculate time to accumulate with the base energy rate
        double time_to_accumulate = missing_energy / energy_rate;
        
        // Add a smaller buffer (10% instead of 20%)
        time_to_accumulate *= 1.1;
        
        double energy_start_time = current_time + time_to_accumulate;
        
        if (energy_start_time > earliest_start_time) {
          earliest_start_time = energy_start_time;
          fprintf(stderr, "Job %s needs %.2f more energy, waiting until %.2f (considering %.2f J from future)\n",
                  first_job->job_id.c_str(), missing_energy, earliest_start_time, future_energy);
        }
      }
    }
    
    // Cap the reservation time to prevent looking too far ahead
    double max_future_time = current_time + 5.0;
    if (earliest_start_time > max_future_time) {
      fprintf(stderr, "Capping reservation time from %.2f to %.2f\n", earliest_start_time, max_future_time);
      earliest_start_time = max_future_time;
    }
    
    // Make reservation for the first job
    if (earliest_start_time > current_time) {
      reserve_energy_reducePC(first_job, earliest_start_time, current_time);
    }
    
    // DYNAMIC BACKFILLING: Now try to backfill other jobs that wouldn't delay the first job
    // and prioritize based on waiting time / energy need
    
    // Calculate how many hosts will remain available after first job
    int remaining_hosts = available_hosts; 
    
    // Keep trying to schedule jobs as long as we have resources
    while (remaining_hosts > 0) {
      // Find the best job for backfilling based on priority
      SchedJob* best_job = find_best_job_for_backfill(current_time, remaining_hosts);
      
      if (!best_job || best_job == first_job) break;
      
      // Check if we have enough energy
      if (has_enough_energy(best_job, current_time)) {
        if (allocate_hosts_for_job(best_job)) {
          // Convert the allocated hosts to interval set for Batsim
          IntervalSet hosts;
          for (int host_id : best_job->allocated_hosts) {
            hosts.insert(host_id);
          }
          
          mb->add_execute_job(best_job->job_id, hosts.to_string_hyphen());
          
          // Set metadata for the job
          best_job->start_time = current_time;
          best_job->expected_end_time = current_time + best_job->walltime;
          
          // We no longer subtract energy upfront - track gradually
          fprintf(stderr, "Backfilling job %s, need %.2f J, available: %.2f J\n", 
                  best_job->job_id.c_str(), estimate_job_energy(best_job), available_energy);
          
          // Add to running jobs
          (*running_jobs)[best_job->job_id] = best_job;
          
          // Remove from queue
          auto it = std::find(jobs->begin(), jobs->end(), best_job);
          if (it != jobs->end()) {
            jobs->erase(it);
          }
          
          any_job_scheduled = true;
          last_job_start_time = current_time;
          emergency_mode = false;
          consecutive_scheduling_failures = 0;
          
          // Update remaining hosts for next iteration
          remaining_hosts -= best_job->nb_hosts;
        } else {
          break; // Couldn't allocate hosts (fragmentation)
        }
      } else {
        break; // Not enough energy for this job
      }
    }
  }
  
  // If no jobs were scheduled and we have resources available, increment the failure counter
  if (!any_job_scheduled && available_hosts > 0) {
    consecutive_scheduling_failures++;
    fprintf(stderr, "Failed to schedule any job with %d hosts available, failure count: %d/%d\n", 
            available_hosts, consecutive_scheduling_failures, max_failures_before_override);
    
    // If we've failed too many times, force schedule a job regardless of energy
    if (consecutive_scheduling_failures >= max_failures_before_override) {
      fprintf(stderr, "CRITICAL: Forcing job scheduling after %d failed attempts\n", consecutive_scheduling_failures);
      
      // Find the best job to force schedule (prioritize small jobs when energy is low)
      SchedJob* best_job = nullptr;
      if (available_energy < energy_rate * 5.0) {
        // Energy is low, schedule smallest job
        best_job = find_smallest_job();
      } else {
        // Energy is reasonable, schedule by priority
        best_job = find_best_job_for_backfill(current_time, available_hosts);
      }
      
      if (best_job && force_schedule_job(best_job, current_time)) {
        return true;
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
  // decisions will now use the current time, as received from batsim
  mb->clear(current_time);

  // Count running hosts
  int running_hosts_count = 0;
  for (size_t i = 0; i < host_used->size(); i++) {
    if ((*host_used)[i]) {
      running_hosts_count++;
    }
  }
  int idle_hosts = platform_nb_hosts - running_hosts_count;

  // Check for potential deadlock: if we've been idle too long, force scheduling
  if (running_jobs->empty() && !jobs->empty() && current_time > 10.0) {
    // If we've been waiting with no running jobs for more than 10 seconds, enter emergency mode
    if (!emergency_mode) {
      emergency_mode = true;
      fprintf(stderr, "CRITICAL: No running jobs detected for too long. Entering emergency mode!\n");
      
      // Add a big energy boost in emergency mode
      double boost = energy_rate * 15.0;  // 15 seconds worth of energy
      available_energy += boost;
      fprintf(stderr, "CRITICAL ENERGY BOOST: Adding %.2f J, now available: %.2f J\n", boost, available_energy);
    }
  }

  // Check if we've gone more than 10 seconds since the last job started
  if (!jobs->empty() && current_time - last_job_start_time > 10.0) {
    // Increment scheduling failures if we haven't scheduled anything for a while
    consecutive_scheduling_failures = std::max(consecutive_scheduling_failures, max_failures_before_override);
    
    // Add periodic energy boosts to ensure progress
    double boost = energy_rate * 5.0;
    available_energy += boost;
    fprintf(stderr, "PERIODIC ENERGY BOOST: Adding %.2f J, now available: %.2f J\n", boost, available_energy);
  }

  // Flag to track if we need to check for jobs to schedule after processing events
  bool should_schedule = false;

  // traverse all events that have just been received
  auto nb_events = parsed->events()->size();
  for (unsigned int i = 0; i < nb_events; ++i) {
    auto event = (*parsed->events())[i];
    fprintf(stderr, "reducePC_IDLE received event type='%s'\n", batprotocol::fb::EnumNamesEvent()[event->event_type()]);
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
        
        // Recalculate energy budget with the actual number of hosts
        if (energy_budget_active) {
          double period_duration = budget_end_time - budget_start_time;
          double budget_factor = 1.1; // 110% budget to allow some flexibility
          
          total_energy_budget = budget_factor * platform_nb_hosts * P_comp * period_duration;
          energy_rate = total_energy_budget / period_duration;
          reduced_energy_rate = energy_rate;
          
          fprintf(stderr, "Energy budget recalculated: %.2f joules, rate: %.2f W\n", 
                 total_energy_budget, energy_rate);
        }
        
        // Add initial startup energy to get things moving
        available_energy += energy_rate * 5.0;
        fprintf(stderr, "Adding startup energy: %.2f J\n", energy_rate * 5.0);
        
        last_job_start_time = current_time;
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
            fprintf(stderr, "Time: %.2f, Restoring energy rate after job completion\n", current_time);
          }
        }
      } break;
      // all jobs have been submitted
      case fb::Event_AllStaticJobsHaveBeenSubmittedEvent: {
        should_schedule = true;
        fprintf(stderr, "All jobs submitted at time %.2f\n", current_time);
        
        // Enter emergency mode if all jobs submitted but none running
        if (running_jobs->empty() && !jobs->empty()) {
          emergency_mode = true;
          fprintf(stderr, "EMERGENCY MODE: All jobs submitted but none running\n");
          
          // Add an energy boost
          double boost = energy_rate * 10.0;
          available_energy += boost;
          fprintf(stderr, "Adding emergency energy boost: %.2f J, now available: %.2f J\n", boost, available_energy);
        }
      } break;
      default: break;
    }
  }

  // Try to schedule jobs if we need to
  if (should_schedule) {
    fprintf(stderr, "Attempting to schedule jobs at time %.2f\n", current_time);
    if (!try_schedule_jobs(current_time)) {
      // If we failed to schedule any job, force schedule in an emergency if it's been too long
      if (current_time > 12.0 && !jobs->empty() && idle_hosts > 0) {
        // We're at least 12 seconds into the simulation, have idle resources, and can't schedule normally
        fprintf(stderr, "LAST RESORT: Force scheduling job at time %.2f\n", current_time);
        
        // Find the best job to schedule
        SchedJob* best_job = find_best_job_for_backfill(current_time, idle_hosts);
        if (best_job) {
          force_schedule_job(best_job, current_time);
        }
      }
    }
  }

  // serialize decisions that have been taken into the output parameters of the function
  mb->finish_message(current_time);
  serialize_message(*mb, !format_binary, const_cast<const uint8_t **>(decisions), decisions_size);
  return 0;
}