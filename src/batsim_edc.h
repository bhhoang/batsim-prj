// This is free and unencumbered software released into the public domain.
// For more information, please refer to <http://unlicense.org/>

// This file describes the C API you can use to make your decision components
// (schedulers, workload injectors...) callable by Batsim as dynamic libraries.

#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>

// These are the flags supported by batsim_edc_init() argument.
#define BATSIM_EDC_FORMAT_BINARY 0x1 // Format is flatbuffers's binary format. Messages are pointers to buffers generated by a flatbuffers library.
#define BATSIM_EDC_FORMAT_JSON 0x2 // Format is flatbuffer's JSON format. Messages are NULL-terminated C strings with JSON content.

/**
 * @brief The batsim_edc_init() function is called by Batsim to initialize your external decision component.
 * @details This is typically used to initialize global data structures to de/serialize messages and to take decisions later on.
 *
 * @param[in] data The initialization data of your decision component. This is retrieved from Batsim's command-line arguments.
 * @param[in] size The size of your initialization data. This is retrieved from Batsim's command-line arguments.
 * @param[in] flags These flags tell you additional information on how to communicate with Batsim.
 *                  Currently, this is only used to know which data format should be used (flatbuffers's binary format or flatbuffer's JSON format).
 * @return Zero if and only if you could initialize yourself successfully.
 */
uint8_t batsim_edc_init(const uint8_t * data, uint32_t size, uint32_t flags);

/**
 * @brief The batsim_edc_deinit() function is called by Batsim when it stops calling your decision component.
 * @details This is typically used to deallocate any memory allocated by batsim_edc_init() or batsim_edc_take_decisions().
 * @return Zero if and only if you could deinitialize yourself successfully.
 */
uint8_t batsim_edc_deinit();

/**
 * @brief The batsim_edc_take_decisions() function is called by Batsim when it asks you to take decisions.
 *
 * @param[in] what_happened A Batsim protocol message that contains what happened in the simulation since the previous call to your decision component
 *                          (that is to say, since last batsim_edc_take_decisions() call or since the initial batsim_edc_init() at the beginning of the simulation).
 *                          The message format depends on what flags were given to batsim_edc_init().
 * @param[in] what_happened_size The size (in bytes) of the what_happened input buffer.
 * @param[out] decisions A Batsim protocol message that contains the decisions taken by this function.
 *                       The buffer should be formatted according to the flags given to batsim_edc_init().
 *                       This buffer must be allocated by you and must persist in memory at least until the next batsim_edc_take_decisions() or batsim_edc_deinit() call.
 * @param[out] decisions_size The size (in bytes) of the decisions output buffer.
 * @return Zero if and only if you could take decisions.
 */
uint8_t batsim_edc_take_decisions(const uint8_t * what_happened, uint32_t what_happened_size, uint8_t ** decisions, uint32_t * decisions_size);

#ifdef __cplusplus
}
#endif
