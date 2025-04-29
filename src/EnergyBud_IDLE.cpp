#include <cstdint>
#include <list>
#include <set>
#include <unordered_map>
#include <string>
#include <sstream>
#include <batprotocol.hpp>
#include "batsim_edc.h"
#include <iostream>
using namespace batprotocol;

struct SchedJob {
    std::string job_id;
    uint32_t nb_hosts;
    double walltime;
};

MessageBuilder* mb = nullptr;
bool format_binary = true;
std::list<SchedJob*>* jobs = nullptr;
std::unordered_map<std::string, SchedJob*> running_jobs;
std::unordered_map<std::string, std::set<uint32_t>> job_allocations;
uint32_t platform_nb_hosts = 0;
std::set<uint32_t> available_res;

// EnergyBud variables
double pourcentage_budget = 1.0;
double max_energy_budget = 1500.8;
double energy_budget = max_energy_budget * pourcentage_budget;
double energy_consumed = 0.0;
double energy_available = 0.0;
const double power_per_host = 203.12;       // P_comp from paper
const double idle_power_per_host = 100.0;   // P_idle from paper
const double off_power_per_host = 9.75;     // P_off from paper
const double monitoring_interval = 600.0;   // 10 minutes as in paper  --> not use here
double last_energy_update_time = 0.0;
double budget_period_duration = 600.0;   // 10 min
double budget_start_time = 0.0;

// Reservation
std::string reserved_job_id = "";
double reserved_energy = 0.0;
double reserved_time_end = 0.0;
double elapsed;

std::string resources_to_str(const std::set<uint32_t>& resources) {
    std::stringstream ss;
    for (auto it = resources.begin(); it != resources.end(); ++it) {
        if (it != resources.begin()) ss << ",";
        ss << *it;
    }
    return ss.str();
}

double estimated_energy(SchedJob* job) {
    return job->nb_hosts * power_per_host * (job->walltime / 3600.0);
}

void update_energy(double current_time) {
    if (budget_start_time == 0.0) {
        budget_start_time = current_time;
        last_energy_update_time = current_time;
        return;
    }

    elapsed = current_time - last_energy_update_time;
    if (elapsed <= 0) return;

    // Rule 1: Make energy available gradually
    double energy_released = (energy_budget / budget_period_duration) * elapsed;
    energy_available += energy_released;

    // Calculate current power consumption
    double active_hosts = platform_nb_hosts - available_res.size();
    double current_power = (active_hosts * power_per_host) + 
                          (available_res.size() * idle_power_per_host);

    // Update energy consumption (convert from watts to watt-hours)
    double energy_used = current_power * (elapsed / 3600.0);
    energy_consumed += energy_used;
    energy_available -= energy_used;

    // Ensure we don't go negative on available energy
    if (energy_available < 0) {
        energy_available = 0;
    }

    last_energy_update_time = current_time;
}

bool has_enough_energy(SchedJob* job, double current_time) {
    // Calculate available energy considering reservations
    double available = energy_available;
    if (!reserved_job_id.empty() && reserved_job_id != job->job_id) {
        available -= reserved_energy;
    }

    // Rule 2: Ensure enough energy for entire job duration
    double required_energy = estimated_energy(job);
    double time_remaining = budget_period_duration - (current_time - budget_start_time);
    
    // Calculate maximum energy we could get during job execution
    double max_possible_energy = available + 
                               (energy_budget / budget_period_duration) * job->walltime;
    
    return (required_energy <= max_possible_energy) && (available >= 0);
}

bool can_backfill(SchedJob* job, double current_time) {
    // Can backfill if job fits before reservation ends or if no reservation exists
    return (reserved_job_id.empty()) || 
           (current_time + job->walltime <= reserved_time_end);
}

void allocate_and_launch(SchedJob* job, double current_time) {
    if (available_res.size() < job->nb_hosts) return;

    // Allocate resources
    std::set<uint32_t> job_resources;
    auto it = available_res.begin();
    for (uint32_t i = 0; i < job->nb_hosts; ++i) {
        job_resources.insert(*it);
        it = available_res.erase(it);
    }

    // Update running jobs and allocations
    running_jobs[job->job_id] = job;
    job_allocations[job->job_id] = job_resources;

    // Deduct energy from available pool
    energy_available -= estimated_energy(job);

    printf("[%.1f] Launching job %s on resources %s (energy: %.1f Wh)\n",
           current_time, job->job_id.c_str(), resources_to_str(job_resources).c_str(), 
           estimated_energy(job));

    mb->add_execute_job(job->job_id, resources_to_str(job_resources));
}

void reserve_for_first_job(SchedJob* job, double current_time) {
    reserved_energy = estimated_energy(job);
    reserved_time_end = current_time + job->walltime;
    reserved_job_id = job->job_id;
    printf("[%.1f] Reserved for job %s: %.1f Wh until %.1f\n", 
           current_time, job->job_id.c_str(), reserved_energy, reserved_time_end);
}

void cancel_reservations() {
    reserved_job_id = "";
    reserved_energy = 0.0;
    reserved_time_end = 0.0;
}

uint8_t batsim_edc_init(const uint8_t* data, uint32_t size, uint32_t flags) {
    format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
    mb = new MessageBuilder(!format_binary);
    jobs = new std::list<SchedJob*>();
    return 0;
}

uint8_t batsim_edc_deinit() {
    delete mb;
    if (jobs) {
        for (auto* job : *jobs) delete job;
        delete jobs;
    }
    running_jobs.clear();
    job_allocations.clear();
    return 0;
}

uint8_t batsim_edc_take_decisions(
    const uint8_t* what_happened,
    uint32_t what_happened_size,
    uint8_t** decisions,
    uint32_t* decisions_size) {

    auto* parsed = deserialize_message(*mb, !format_binary, what_happened);
    double current_time = parsed->now();
    mb->clear(current_time);

    for (unsigned int i = 0; i < parsed->events()->size(); ++i) {
        auto event = (*parsed->events())[i];
        switch (event->event_type()) {
            case fb::Event_BatsimHelloEvent:
                mb->add_edc_hello("EnergyBud", "1.0.0");
                break;

            case fb::Event_SimulationBeginsEvent: {
                auto simu_begins = event->event_as_SimulationBeginsEvent();
                platform_nb_hosts = simu_begins->computation_host_number();
                available_res.clear();
                for (uint32_t i = 0; i < platform_nb_hosts; i++) {
                    available_res.insert(i);
                }
                printf("[%.1f] Platform initialized with %d hosts\n", 
                       current_time, platform_nb_hosts);
            } break;
            
            case fb::Event_JobSubmittedEvent: {
                auto parsed_job = event->event_as_JobSubmittedEvent();
                auto job = new SchedJob{
                    parsed_job->job_id()->str(),
                    static_cast<uint8_t>(parsed_job->job()->resource_request()), // FIX: avoid warning narrowing
                    parsed_job->job()->walltime(),

                    
                };
                if (job->nb_hosts > platform_nb_hosts) {
                    mb->add_reject_job(job->job_id);
                    delete job;
                } else {
                    jobs->push_back(job);
                    printf("[%.1f] Job %s submitted (%d hosts, %.1fs)\n",
                           current_time, job->job_id.c_str(),
                           job->nb_hosts, job->walltime);
                }
            } break;
            
            case fb::Event_JobCompletedEvent: {
                auto job_id = event->event_as_JobCompletedEvent()->job_id()->str();
                if (running_jobs.count(job_id)) {
                    for (uint32_t host : job_allocations[job_id]) {
                        available_res.insert(host);
                    }
                    running_jobs.erase(job_id);
                    job_allocations.erase(job_id);
                    printf("[%.1f] Job %s completed\n", current_time, job_id.c_str());
                }
                if (job_id == reserved_job_id) {
                    cancel_reservations();
                }
            } break;
            
            default: break;
        }
    }

    update_energy(current_time);

    // 1. try to run all possible jobs
    for (auto it = jobs->begin(); it != jobs->end();) {
        SchedJob* job = *it;
        if (available_res.size() >= job->nb_hosts && has_enough_energy(job, current_time)) {
            allocate_and_launch(job, current_time);
            it = jobs->erase(it);
        } else {
            ++it;
        }
    }

    // 2. if first job blocked, reserve and try to run it
    if (!jobs->empty() && reserved_job_id.empty()) {
        SchedJob* first_job = jobs->front();
        reserve_for_first_job(first_job, current_time);

        if (available_res.size() >= first_job->nb_hosts && has_enough_energy(first_job, current_time)) {
            jobs->pop_front();
            allocate_and_launch(first_job, current_time);
            cancel_reservations();
        }
    }

    // 3. try backfilling
    if (!reserved_job_id.empty()) {
        for (auto it = jobs->begin(); it != jobs->end();) {
            SchedJob* job = *it;
            if (job->job_id != reserved_job_id &&
                available_res.size() >= job->nb_hosts &&
                has_enough_energy(job, current_time) &&
                can_backfill(job, current_time)) {
                allocate_and_launch(job, current_time);
                it = jobs->erase(it);
            } else {
                ++it;
            }
        }
    }

    printf("[%.1f] Status: %lu jobs queued, %lu/%d hosts free, Energy: %.1f/%.1f Wh (reserved: %.1f)\n",
           current_time, jobs->size(), available_res.size(), platform_nb_hosts,
           energy_available, energy_budget, reserved_energy);

    mb->finish_message(current_time);
    serialize_message(*mb, !format_binary, const_cast<const uint8_t**>(decisions), decisions_size);
    return 0;
}
