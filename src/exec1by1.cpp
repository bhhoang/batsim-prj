#include <cstdint>
#include <list>

#include <batprotocol.hpp>
#include <intervalset.hpp>

#include "batsim_edc.h"

using namespace batprotocol;

struct SchedJob {
  std::string job_id;
  uint8_t nb_hosts;
};

MessageBuilder * mb = nullptr;
bool format_binary = true; // whether flatbuffers binary or json format should be used
std::list<SchedJob*> * jobs = nullptr;
SchedJob * currently_running_job = nullptr;
uint32_t platform_nb_hosts = 0;

// this function is called by batsim to initialize your decision code
uint8_t batsim_edc_init(const uint8_t * data, uint32_t size, uint32_t flags) {
  format_binary = ((flags & BATSIM_EDC_FORMAT_BINARY) != 0);
  if ((flags & (BATSIM_EDC_FORMAT_BINARY | BATSIM_EDC_FORMAT_JSON)) != flags) {
    printf("Unknown flags used, cannot initialize myself.\n");
    return 1;
  }

  mb = new MessageBuilder(!format_binary);
  jobs = new std::list<SchedJob*>();

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

  return 0;
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

  // clear data structures to take the next decisions.
  // decisions will now use the current time, as received from batsim
  mb->clear(parsed->now());

  // traverse all events that have just been received
  auto nb_events = parsed->events()->size();
  for (unsigned int i = 0; i < nb_events; ++i) {
    auto event = (*parsed->events())[i];
    printf("exec1by1 received event type='%s'\n", batprotocol::fb::EnumNamesEvent()[event->event_type()]);
    switch (event->event_type()) {
      // protocol handshake
      case fb::Event_BatsimHelloEvent: {
        mb->add_edc_hello("exec1by1", "0.1.0");
      } break;
      // batsim tells you that the simulation starts, providing you various initialization information
      case fb::Event_SimulationBeginsEvent: {
        auto simu_begins = event->event_as_SimulationBeginsEvent();
        platform_nb_hosts = simu_begins->computation_host_number();
      } break;
      // a job has just been submitted
      case fb::Event_JobSubmittedEvent: {
        auto parsed_job = event->event_as_JobSubmittedEvent();
        auto job = new SchedJob();
        job->job_id = parsed_job->job_id()->str();

        job->nb_hosts = parsed_job->job()->resource_request();
        if (job->nb_hosts > platform_nb_hosts) {
          mb->add_reject_job(job->job_id);
          delete job;
        }
        else {
          jobs->push_back(job);
        }
      } break;
      // a job has just completed
      case fb::Event_JobCompletedEvent: {
        delete currently_running_job;
        currently_running_job = nullptr;
      } break;
      default: break;
    }
  }

  // run one job if the platform is unused and if there is a job waiting
  if (currently_running_job == nullptr && !jobs->empty()) {
    currently_running_job = jobs->front();
    jobs->pop_front();
    auto hosts = IntervalSet(IntervalSet::ClosedInterval(0, currently_running_job->nb_hosts-1));
    mb->add_execute_job(currently_running_job->job_id, hosts.to_string_hyphen());
  }

  // serialize decisions that have been taken into the output parameters of the function (decisions, decisions_size)
  mb->finish_message(parsed->now());
  serialize_message(*mb, !format_binary, const_cast<const uint8_t **>(decisions), decisions_size);
  return 0;
}
