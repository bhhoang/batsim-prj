//Premier code

#include <cstdint>
#include <list>
#include <set>
#include <unordered_map>
#include <batprotocol.hpp>
#include "batsim_edc.h"

using namespace batprotocol;

struct SchedJob {
    std::string job_id;
    uint8_t nb_hosts;
    double walltime; // Durée estimée d'exécution
};  

MessageBuilder * mb = nullptr;
bool format_binary = true;
std::list<SchedJob*> * jobs = nullptr;
std::unordered_map<std::string, SchedJob*> running_jobs;
std::unordered_map<std::string, std::set<uint32_t>> job_allocations;
uint32_t platform_nb_hosts = 0;
std::set<uint32_t> available_res;
double shadow_time = 0.0; // Temps maximum où le premier job peut être retardé

double P_IDLE_M = 100.0, P_COMP_M = 203.12, P_IDLE_A = 95, P_COMP_A = 190.74;
double ENERGY_BUDGET = 0;
double PERIOD_LENGTH = 600;  // 10 Min
double power_limit = 0 ;
double pourcentage_budget = 1.0; //100% pour le moment 
double current_power = 0;

// Initialisation
uint8_t batsim_edc_init(const uint8_t * data, uint32_t size, uint32_t flags) {
    format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
    if ((flags & (BATSIM_EDC_FORMAT_BINARY | BATSIM_EDC_FORMAT_JSON)) != flags) {
        printf("Unknown flags used, cannot initialize myself.\n");
        return 1;
    }

    mb = new MessageBuilder(!format_binary);
    jobs = new std::list<SchedJob*>();

    return 0;
}

// Nettoyage mémoire en fin de simulation
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

    running_jobs.clear();
    job_allocations.clear();
    return 0;
}

// Gestion des décisions
uint8_t batsim_edc_take_decisions(
    const uint8_t * what_happened,
    uint32_t what_happened_size,
    uint8_t ** decisions,
    uint32_t * decisions_size)
{
    (void) what_happened_size;
    auto * parsed = deserialize_message(*mb, !format_binary, what_happened);
    mb->clear(parsed->now());

    auto nb_events = parsed->events()->size();
    for (unsigned int i = 0; i < nb_events; ++i) {
        auto event = (*parsed->events())[i];
        printf("easy_backfill received event type='%s'\n", batprotocol::fb::EnumNamesEvent()[event->event_type()]);

        switch (event->event_type()) {
            case fb::Event_BatsimHelloEvent: {
                mb->add_edc_hello("easy_backfill", "1.0.0");
            } break;

            case fb::Event_SimulationBeginsEvent: {
                auto simu_begins = event->event_as_SimulationBeginsEvent();
                platform_nb_hosts = simu_begins->computation_host_number();
                printf("nb machines %d", platform_nb_hosts);
                
                // Init des ressources disponibles
                for (uint32_t i = 0; i < platform_nb_hosts; i++) {
                    available_res.insert(i);
                }
                current_power = platform_nb_hosts * P_IDLE_A;
                ENERGY_BUDGET = platform_nb_hosts * P_COMP_M * pourcentage_budget; //3 jour en seconde = 259200
                power_limit = ENERGY_BUDGET;  // PERIOD_LENGTH;
            printf("conso de base at beginning = %lf , power limit = %lf \n",current_power,power_limit);
            } break;

            case fb::Event_JobSubmittedEvent: {
                auto parsed_job = event->event_as_JobSubmittedEvent();
                auto job = new SchedJob();
                job->job_id = parsed_job->job_id()->str();
                job->nb_hosts = parsed_job->job()->resource_request();
                job->walltime = parsed_job->job()->walltime(); // Récupération du walltime

                if (job->nb_hosts > platform_nb_hosts) {
                    mb->add_reject_job(job->job_id);
                    delete job;
                } else {
                    jobs->push_back(job);
                    if (jobs->size() == 1) {
                        shadow_time = job->walltime; // Premier job : son temps de walltime est le temps limite
                    }
                    double con = job->nb_hosts * (P_COMP_A -P_IDLE_A);
                    printf("conso de base du job = %lf \n",con);
                }
            } break;

            case fb::Event_JobCompletedEvent: {
                auto parsed_job = event->event_as_JobCompletedEvent();
                std::string completed_job_id = parsed_job->job_id()->str();

                if (running_jobs.count(completed_job_id)) {
                    SchedJob* completed_job = running_jobs[completed_job_id];

                    // Libération des ressources
                    for (uint32_t host : job_allocations[completed_job_id]) {
                        available_res.insert(host);
                    }

                    current_power = available_res.size() * P_IDLE_A + (platform_nb_hosts - available_res.size()) * P_COMP_A;
                    printf("conso after finishing a job = %lf \n",current_power);
                    running_jobs.erase(completed_job_id);
                    job_allocations.erase(completed_job_id);
                    delete completed_job;
                }
            } break;

            default: break;
        }
    }

    // BACKFILLING : Exécuter les jobs sans retarder le premier de la file
    if (!jobs->empty()) {
        SchedJob* first_job = jobs->front();
        std::set<uint32_t> job_resources;
        double soon_power = current_power + first_job->nb_hosts * (P_COMP_A - P_IDLE_A);
        bool first_job_can_run = false;
        if(available_res.size() >= first_job->nb_hosts && soon_power <= power_limit){
            first_job_can_run = true;
        }
        if(soon_power>power_limit){
            printf("this job %s ask too musch energy %lf over %lf \n", first_job->job_id.c_str(), soon_power, power_limit);
        }
        for (auto it = std::next(jobs->begin()); it != jobs->end(); ++it) {
            SchedJob* backfill_candidate = *it;
            double backfill_power = current_power + backfill_candidate->nb_hosts * (P_COMP_A - P_IDLE_A);
            if (available_res.size() >= backfill_candidate->nb_hosts && backfill_candidate->walltime <= shadow_time && backfill_power <= power_limit ){ 
                std::set<uint32_t> job_resources;
                auto res_it = available_res.begin();
                for (uint8_t i = 0; i < backfill_candidate->nb_hosts; ++i, ++res_it) {
                    job_resources.insert(*res_it);
                }
            
                bool resources_available = true;
                for (uint32_t res : job_resources) {
                    if (available_res.find(res) == available_res.end()) {
                        resources_available = false;
                        break;
                    }
                }
                current_power = available_res.size() * P_IDLE_A + (platform_nb_hosts - available_res.size()) * P_COMP_A;
                if (resources_available && backfill_power <= power_limit) {
                    // Allouer les ressources
                    for (uint32_t res : job_resources) {
                        available_res.erase(res);
                    }
                    current_power = backfill_power; 

                    running_jobs[backfill_candidate->job_id] = backfill_candidate;
                    job_allocations[backfill_candidate->job_id] = job_resources;
            
                    std::string resources_str;
                    for (auto it = job_resources.begin(); it != job_resources.end(); ++it) {
                        if (it != job_resources.begin()) resources_str += ",";
                        resources_str += std::to_string(*it);
                    }
            
                    printf("Backfilling job %s to resources: %s and has a conso of %lf new power : %lf over total %lf\n",
                    backfill_candidate->job_id.c_str(), resources_str.c_str(),backfill_power,current_power,power_limit);
                    mb->add_execute_job(backfill_candidate->job_id, resources_str);
                    jobs->erase(it);
                    break;
                }
            }
            if(backfill_power>power_limit){
                printf("this job %s ask too musch energy %lf over %lf \n", backfill_candidate->job_id.c_str(), soon_power, power_limit);
            }
        }

        // Si aucune exécution en backfilling, exécuter le premier job
        if (first_job_can_run) {
            auto it = available_res.begin();
            for (uint8_t i = 0; i < first_job->nb_hosts; ++i, ++it) {
                job_resources.insert(*it);
            }
            for (uint32_t res : job_resources) {
                available_res.erase(res);
            }
            current_power = soon_power;
            running_jobs[first_job->job_id] = first_job;
            job_allocations[first_job->job_id] = job_resources;

            std::string resources_str;
            for (auto it = job_resources.begin(); it != job_resources.end(); ++it) {
                if (it != job_resources.begin()) resources_str += ",";
                resources_str += std::to_string(*it);
            }

            printf("Assigning first job %s to resources: %s and has a conso of %lf new power : %lf over total %lf\n",
            first_job->job_id.c_str(), resources_str.c_str(),soon_power,current_power,power_limit);
            mb->add_execute_job(first_job->job_id, resources_str);
            jobs->pop_front();
        }
    }

    mb->finish_message(parsed->now());
    serialize_message(*mb, !format_binary, const_cast<const uint8_t **>(decisions), decisions_size);
    return 0;
}
/*The second one is a powercapped EASY Backfilling. A power limit
is set during the whole energy budget period, which is set to
the energy budget (J) divided by the period length (s). The
platform energy consumption is estimated with P̃platf orm =
nidle × P̃idle + ncomp × P̃comp , where nidle is the number
of idle nodes and ncomp is the number of nodes which are
computing jobs. This algorithm is roughly the same as EASY
Backfilling, but jobs are not executed if they cause P̃platf orm
to be greater than the power limit.*/