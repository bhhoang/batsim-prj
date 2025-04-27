// 2

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
    uint8_t nb_hosts;
    double walltime;
};

// Variables globales
MessageBuilder* mb = nullptr;
bool format_binary = true;
std::list<SchedJob*>* jobs = nullptr;
std::unordered_map<std::string, SchedJob*> running_jobs;
std::unordered_map<std::string, std::set<uint32_t>> job_allocations;
uint32_t platform_nb_hosts = 0;
std::set<uint32_t> available_res;
double shadow_time = 0.0;

// Variables pour EnergyBud
double percentage_budget = 1.0;
double max_energy_budget = 1500.8;
double energy_budget = max_energy_budget * percentage_budget;
double energy_consumed = 0.0;
double energy_available = 0.0;
const double power_per_host = 203.12;
const double idle_power_per_host = 100.0;
const double monitoring_interval = 600.0;
double last_energy_update_time = 0.0;
double budget_period_duration = 600.0;
double budget_start_time = 0.0;

// Réservation pour le premier job uniquement
double reserved_energy = 0.0;
double reserved_time_end = 0.0;
std::string reserved_job_id = "";

uint8_t batsim_edc_init(const uint8_t* data, uint32_t size, uint32_t flags) {
    format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
    if ((flags & (BATSIM_EDC_FORMAT_BINARY | BATSIM_EDC_FORMAT_JSON)) != flags) {
        printf("Unknown flags used, cannot initialize myself.\n");
        return 1;
    }

    mb = new MessageBuilder(!format_binary);
    jobs = new std::list<SchedJob*>();
    return 0;
}

uint8_t batsim_edc_deinit() {
    delete mb;
    mb = nullptr;

    if (jobs != nullptr) {
        for (auto* job : *jobs) delete job;
        delete jobs;
        jobs = nullptr;
    }

    running_jobs.clear();
    job_allocations.clear();
    return 0;
}

std::string resources_to_str(const std::set<uint32_t>& resources) {
    std::stringstream ss;
    for (auto it = resources.begin(); it != resources.end(); ++it) {
        if (it != resources.begin()) ss << ",";
        ss << *it;
    }
    return ss.str();
}

void update_energy(double current_time) {
    if (budget_start_time == 0.0) {
        budget_start_time = current_time;
        last_energy_update_time = current_time;
        energy_budget = max_energy_budget * percentage_budget;
        energy_available = energy_budget / budget_period_duration * monitoring_interval;
        return;
    }

    double elapsed = current_time - last_energy_update_time;
    if (elapsed <= 0) return;

    // Libération progressive de l'énergie
    double energy_released = (energy_budget / budget_period_duration) * elapsed;
    energy_available += energy_released;

    // Consommation estimée
    double active_hosts = platform_nb_hosts - available_res.size();
    double estimated_consumption = (active_hosts * power_per_host + 
                                  available_res.size() * idle_power_per_host) * 
                                  (elapsed / 3600.0);

    energy_consumed += estimated_consumption;
    energy_available -= estimated_consumption;

    last_energy_update_time = current_time;
}

bool has_enough_energy(SchedJob* job, double current_time) {
    double available = energy_available;
    if (reserved_job_id != job->job_id) {
        available -= reserved_energy;
    }
    
    double required_energy = job->nb_hosts * power_per_host * (job->walltime / 3600.0);
    double future_available = available + (energy_budget / budget_period_duration) * job->walltime;
    
    return (required_energy <= future_available) && (available >= 0);
}

bool can_backfill(SchedJob* job, double current_time) {
    return (current_time + job->walltime <= reserved_time_end) || (reserved_job_id.empty());
}

void allocate_and_launch(SchedJob* job, double current_time) {
    if (available_res.size() < job->nb_hosts) {
        printf("Not enough resources for job %s (requested %d, available %lu)\n",
               job->job_id.c_str(), job->nb_hosts, available_res.size());
        return;
    }

    std::set<uint32_t> job_resources;
    auto it = available_res.begin();
    for (uint8_t i = 0; i < job->nb_hosts; ++i) {
        job_resources.insert(*it);
        it = available_res.erase(it);
    }

    running_jobs[job->job_id] = job;
    job_allocations[job->job_id] = job_resources;
    
    double required_energy = job->nb_hosts * power_per_host * (job->walltime / 3600.0);
    energy_available -= required_energy;

    printf("[%.1f] Launching job %s on resources %s (energy: %.1f Wh)\n",
           current_time, job->job_id.c_str(),
           resources_to_str(job_resources).c_str(), required_energy);
    
    mb->add_execute_job(job->job_id, resources_to_str(job_resources));
}

void reserve_for_first_job(SchedJob* job, double current_time) {
    reserved_energy = job->nb_hosts * power_per_host * (job->walltime / 3600.0);
    reserved_time_end = current_time + job->walltime;
    reserved_job_id = job->job_id;
    printf("[%.1f] Reserved for job %s: %.1f Wh until %.1f\n",
           current_time, job->job_id.c_str(), reserved_energy, reserved_time_end);
}

void cancel_reservations() {
    if (!reserved_job_id.empty()) {
        printf("[Canceling reservation for job %s (%.1f Wh freed)]\n",
               reserved_job_id.c_str(), reserved_energy);
        reserved_energy = 0.0;
        reserved_time_end = 0.0;
        reserved_job_id = "";
    }
}

uint8_t batsim_edc_take_decisions(
    const uint8_t* what_happened,
    uint32_t what_happened_size,
    uint8_t** decisions,
    uint32_t* decisions_size) {
    
    auto* parsed = deserialize_message(*mb, !format_binary, what_happened);
    double current_time = parsed->now();
    mb->clear(current_time);

    // Traitement des événements
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
                    parsed_job->job()->resource_request(),
                    parsed_job->job()->walltime()
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
                    printf("[%.1f] Job %s completed, %lu hosts freed\n",
                           current_time, job_id.c_str(), job_allocations[job_id].size());
                    job_allocations.erase(job_id);

                }
                // Ajouter dans JobCompletedEvent
                if (job_id == reserved_job_id) {
                    cancel_reservations();
                }
            } break;
            
            default: break;
        }
    }
    
    update_energy(current_time);

    // 1. Essayer de lancer les jobs qui peuvent s'exécuter immédiatement
    auto it = jobs->begin();
    while (it != jobs->end()) {
        SchedJob* job = *it;
        if (available_res.size() >= job->nb_hosts && has_enough_energy(job, current_time)) {
            allocate_and_launch(job, current_time);
            it = jobs->erase(it);
        } else {
            ++it;
        }
    }

    // 2. Gestion du premier job bloquant
    if (!jobs->empty() && reserved_job_id.empty()) {
        SchedJob* first_job = jobs->front();
        jobs->pop_front();
        
        if (available_res.size() >= first_job->nb_hosts && 
            has_enough_energy(first_job, current_time)) {
            allocate_and_launch(first_job, current_time);
        } else {
            reserve_for_first_job(first_job, current_time);
            jobs->push_front(first_job);
        }
    }

    // 3. Backfilling
    if (!reserved_job_id.empty()) {
        it = jobs->begin();
        while (it != jobs->end()) {
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

    // 4. Vérifier si le job réservé peut maintenant démarrer
    if (!reserved_job_id.empty() && !jobs->empty() && 
        jobs->front()->job_id == reserved_job_id) {
        SchedJob* first_job = jobs->front();
        if (available_res.size() >= first_job->nb_hosts && 
            has_enough_energy(first_job, current_time)) {
            jobs->pop_front();
            allocate_and_launch(first_job, current_time);
            cancel_reservations();
        }
    }

    // Logs d'état
    printf("[%.1f] Status: %lu jobs queued, %lu/%d hosts free, Energy: %.1f/%.1f Wh (reserved: %.1f)\n",
           current_time, jobs->size(), available_res.size(), platform_nb_hosts,
           energy_available, energy_budget, reserved_energy);

    mb->finish_message(current_time);
    serialize_message(*mb, !format_binary, const_cast<const uint8_t**>(decisions), decisions_size);
    return 0;
}
