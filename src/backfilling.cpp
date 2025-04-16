#include <cstdint>
#include <list>
#include <set>
#include <batprotocol.hpp>
#include <intervalset.hpp>
#include "batsim_edc.h"

using namespace batprotocol;

struct SchedJob {
    std::string job_id;
    uint8_t nb_resources;
    IntervalSet allocated_resources;
    double walltime;
    double submit_time;
    double predicted_start_time;
};

struct JobCompletion {
    std::string job_id;
    double completion_time;
    IntervalSet resources;
};

MessageBuilder* mb = nullptr;
bool format_binary = true;
std::list<SchedJob*>* waiting_jobs = nullptr;
std::map<std::string, SchedJob*> running_jobs;  // Track multiple running jobs
std::vector<JobCompletion> future_job_completions;
uint32_t platform_nb_resources = 0;
IntervalSet available_resources;  // Track available resources globally
double current_time = 0.0;

// Helper function to find available resources for a job
bool find_available_resources(uint8_t nb_resources_needed, IntervalSet& result) {
    if (available_resources.size() < nb_resources_needed) {
        return false;
    }
    
    // Convert the available_resources to string and parse it to get ranges
    std::string resources_str = available_resources.to_string_hyphen();
    size_t pos = 0;
    IntervalSet allocated;
    uint8_t count = 0;
    
    while (pos < resources_str.length() && count < nb_resources_needed) {
        // Skip any spaces or commas
        while (pos < resources_str.length() && (resources_str[pos] == ' ' || resources_str[pos] == ',')) {
            pos++;
        }
        
        // Parse start number
        size_t next_pos;
        int start = std::stoi(resources_str.substr(pos), &next_pos);
        pos += next_pos;
        
        // Check if it's a range
        int end = start;
        if (pos < resources_str.length() && resources_str[pos] == '-') {
            pos++; // Skip the hyphen
            end = std::stoi(resources_str.substr(pos), &next_pos);
            pos += next_pos;
        }
        
        // Add resources from this range until we have enough
        for (int i = start; i <= end && count < nb_resources_needed; ++i) {
            allocated.insert(i);
            count++;
        }
    }
    
    if (count >= nb_resources_needed) {
        result = allocated;
        return true;
    }
    
    return false;
}

double predict_job_start_time(const SchedJob* job) {
    IntervalSet temp_resources;
    if (find_available_resources(job->nb_resources, temp_resources)) {
        return current_time;
    }

    // Create timeline for resources' availability
    std::map<double, int> timeline;
    timeline[current_time] = available_resources.size();

    for (const auto &completed_job : future_job_completions) {
        if (completed_job.completion_time > current_time) {
            timeline[completed_job.completion_time] = available_resources.size();
        }
    }

    int current_resources = available_resources.size();
    for (const auto &[time, delta] : timeline) {
        current_resources += delta;
        if (current_resources >= job->nb_resources) {
            return time;
        }
    }

    return current_time + 1e9; // Return a very large number if we can't predict
}
uint8_t batsim_edc_init(const uint8_t* data, uint32_t size, uint32_t flags) {
    format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
    if ((flags & (BATSIM_EDC_FORMAT_BINARY | BATSIM_EDC_FORMAT_JSON)) != flags) {
        printf("Unknown flags used, cannot initialize myself.\n");
        return 1;
    }
    
    mb = new MessageBuilder(!format_binary);
    waiting_jobs = new std::list<SchedJob*>();
    
    (void)data;
    (void)size;
    return 0;
}

uint8_t batsim_edc_deinit() {
    delete mb;
    mb = nullptr;
    
    if (waiting_jobs != nullptr) {
        for (auto* job : *waiting_jobs) {
            delete job;
        }
        delete waiting_jobs;
        waiting_jobs = nullptr;
    }
    
    for (auto& [_, job] : running_jobs) {
        delete job;
    }
    running_jobs.clear();
    
    return 0;
}

void schedule_jobs(){
    if (waiting_jobs->empty()) {
        return;
    }
    auto first_job = waiting_jobs->front();
    first_job->predicted_start_time = predict_job_start_time(first_job);

    IntervalSet allocated_resources;
    if (first_job->predicted_start_time == current_time &&
        find_available_resources(first_job->nb_resources, allocated_resources)) {
        // We found enough resources for this job
        first_job->allocated_resources = allocated_resources;
        available_resources -= allocated_resources;

        // Add execute job decision
        mb->add_execute_job(first_job->job_id, allocated_resources.to_string_hyphen());
        running_jobs[first_job->job_id] = first_job;
        waiting_jobs->pop_front();

        // Add to future job completions
        JobCompletion completion;
        completion.job_id = first_job->job_id;
        completion.completion_time = current_time + first_job->walltime;
        completion.resources = allocated_resources;
        future_job_completions.push_back(completion);
    }

    // Try backfilling with the rest of the jobs
    auto it = std::next(waiting_jobs->begin());
    while (it != waiting_jobs->end()) {
        auto job = *it;
        IntervalSet temp_resources;

        if (find_available_resources(job->nb_resources, temp_resources)) {
            // We found enough resources for this job
            job->allocated_resources = temp_resources;
            available_resources -= temp_resources;

            // Add execute job decision
            mb->add_execute_job(job->job_id, temp_resources.to_string_hyphen());
            running_jobs[job->job_id] = job;
            it = waiting_jobs->erase(it);

            // Add to future job completions
            JobCompletion completion;
            completion.job_id = job->job_id;
            completion.completion_time = current_time + job->walltime;
            completion.resources = temp_resources;
            future_job_completions.push_back(completion);
        } else {
            ++it;
        }
    }
    
}

// FCFS
//void schedule_waiting_jobs() {
//    auto it = waiting_jobs->begin();
//     while (it != waiting_jobs->end()) {
//         auto* job = *it;
//         IntervalSet allocated_resources;
        
//         if (find_available_resources(job->nb_resources, allocated_resources)) {
//             // We found enough resources for this job
//             job->allocated_resources = allocated_resources;
//             available_resources -= allocated_resources;  // Remove allocated resources from available pool
            
//             // Add execute job decision
//             mb->add_execute_job(job->job_id, allocated_resources.to_string_hyphen());
            
//             // Move job to running set
//             running_jobs.insert(job);
//             it = waiting_jobs->erase(it);
//         } else {
//             // Not enough resources for this job, try next one
//             ++it;
//         }
//     }
// }

uint8_t batsim_edc_take_decisions(
    const uint8_t* what_happened,
    uint32_t what_happened_size,
    uint8_t** decisions,
    uint32_t* decisions_size) {
    
    (void)what_happened_size;
    auto* parsed = deserialize_message(*mb, !format_binary, what_happened);
    mb->clear(parsed->now());
    
    auto nb_events = parsed->events()->size();
    for (unsigned int i = 0; i < nb_events; ++i) {
        auto event = (*parsed->events())[i];
        printf("parallel_fcfs received event type='%s'\n", 
               batprotocol::fb::EnumNamesEvent()[event->event_type()]);
        
        switch (event->event_type()) {
            case fb::Event_BatsimHelloEvent: {
                mb->add_edc_hello("parallel_fcfs", "0.1.0");
            } break;
            
            case fb::Event_SimulationBeginsEvent: {
                auto simu_begins = event->event_as_SimulationBeginsEvent();
                platform_nb_resources = simu_begins->computation_host_number();
                // Initialize all resources as available
                available_resources = IntervalSet(IntervalSet::ClosedInterval(0, platform_nb_resources - 1));
            } break;
            
            case fb::Event_JobSubmittedEvent: {
                auto parsed_job = event->event_as_JobSubmittedEvent();
                auto* job = new SchedJob();
                job->job_id = parsed_job->job_id()->str();
                job->nb_resources = parsed_job->job()->resource_request();
                job->walltime = parsed_job->job()->walltime();
                job->submit_time = current_time;
                
                if (job->nb_resources > platform_nb_resources) {
                    mb->add_reject_job(job->job_id);
                    delete job;
                } else {
                    waiting_jobs->push_back(job);
                }
            } break;
            
            case fb::Event_JobCompletedEvent: {
                auto completed = event->event_as_JobCompletedEvent();
                auto job_it = running_jobs.find(completed->job_id()->str());
                if (job_it != running_jobs.end()) {
                    // Return allocated resources to available pool
                    available_resources += job_it->second->allocated_resources;
                    delete job_it->second;
                    running_jobs.erase(job_it);
                }

                future_job_completions.erase(
                    std::remove_if(
                        future_job_completions.begin(),
                        future_job_completions.end(),
                        [completed](const JobCompletion& completion) {
                            return completion.job_id == completed->job_id()->str();
                        }),
                    future_job_completions.end());

            } break;
            
            default:
                break;
        }
    }
    
    // Try to schedule waiting jobs whenever we process events
    schedule_jobs();
    
    // Serialize decisions
    mb->finish_message(parsed->now());
    serialize_message(*mb, !format_binary, const_cast<const uint8_t**>(decisions), decisions_size);
    
    return 0;
}
