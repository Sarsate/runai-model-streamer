#include "gcs/gcs.h"
#include "gcs/client/client.h"
#include "common/client_mgr/client_mgr.h"

#include "common/exception/exception.h"

namespace runai::llm::streamer::impl::gcs
{

inline constexpr char GCSClientName[] = "GCS";
using GCSClientMgr = common::ClientMgr<GCSClient, GCSClientName>;

common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle)
{
    // google-cloud-cpp SDK does not require any global initiaization.
    return common::ResponseCode::Success;
}

common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle)
{
    return common::ResponseCode::Success;
}

common::backend_api::ObjectShutdownPolicy_t obj_get_backend_shutdown_policy()
{
    return common::backend_api::OBJECT_SHUTDOWN_POLICY_ON_PROCESS_EXIT;
}

common::backend_api::ResponseCode_t obj_create_client(common::backend_api::ObjectBackendHandle_t backend_handle,
                                                       const common::backend_api::ObjectClientConfig_t* client_initial_config,
                                                       common::backend_api::ObjectClientHandle_t* out_client_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        // std::cerr << "DEBUG: obj_create_client called" << std::endl;
        *out_client_handle = GCSClientMgr::pop(*client_initial_config);
        // std::cerr << "DEBUG: obj_create_client success" << std::endl;
    }
    catch(const common::Exception & e)
    {
        // std::cerr << "DEBUG ERROR: obj_create_client caught common::Exception: " << static_cast<int>(e.error()) << std::endl;
        ret = e.error();
        *out_client_handle = nullptr;
    }
    catch(const std::exception & e)
    {
        // std::cerr << "DEBUG ERROR: obj_create_client caught std::exception: " << e.what() << std::endl;
        LOG(ERROR) << "Failed to create GCS client";
        ret = common::ResponseCode::FileAccessError;
        *out_client_handle = nullptr;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_remove_client(common::backend_api::ObjectClientHandle_t client_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        if (client_handle)
        {
           GCSClientMgr::push(static_cast<GCSClient *>(client_handle));
        }
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove GCS client";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_remove_all_clients()
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        GCSClientMgr::clear();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove all GCS clients";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_cancel_all_reads()
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        GCSClientMgr::stop();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to stop all GCS clients";
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_request_read(common::backend_api::ObjectClientHandle_t client_handle,
                                                     const char* path,
                                                     common::backend_api::ObjectRange_t range,
                                                     char* destination_buffer,
                                                     common::backend_api::ObjectRequestId_t request_id)
{
    // std::cerr << "DEBUG: obj_request_read called for request " << request_id 
    //           << " range=[" << range.offset << ", " << range.length << "]" << std::endl;
    try
    {
        if (!client_handle)
        {
            // std::cerr << "DEBUG ERROR: obj_request_read called with null client_handle" << std::endl;
            LOG(ERROR) << "Attempt to read with null gcs client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<GCSClient *>(client_handle);
        return ptr->async_read(path, range, destination_buffer, request_id);
    }
    catch(const std::exception& e)
    {
        std::cerr << "DEBUG ERROR: obj_request_read caught exception: " << e.what() << std::endl;
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

common::backend_api::ResponseCode_t obj_wait_for_completions(common::backend_api::ObjectClientHandle_t client_handle,
                                                              common::backend_api::ObjectCompletionEvent_t* event_buffer,
                                                              unsigned int max_events_to_retrieve,
                                                              unsigned int* out_num_events_retrieved,
                                                              common::backend_api::ObjectWaitMode_t wait_mode)
{
    try
    {
        if (!client_handle)
        {
            LOG(ERROR) << "Attempt to get read response with null GCS client";
            return common::ResponseCode::UnknownError;
        }
        if (max_events_to_retrieve == 0)
        {
            LOG(ERROR) << "Attempt to get read response with max_events_to_retrieve = 0";
            return common::ResponseCode::UnknownError;
        }
        if (!event_buffer || !out_num_events_retrieved)
        {
            LOG(ERROR) << "Attempt to get read response with null event_buffer or out_num_events_retrieved";
            return common::ResponseCode::UnknownError;
        }

        // for now reads a single event

        auto ptr = static_cast<GCSClient *>(client_handle);
        auto response = ptr->async_read_response();
        *out_num_events_retrieved = 1;
        event_buffer[0].request_id = response.handle;
        event_buffer[0].response_code = response.ret;
        return common::ResponseCode::Success;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

common::backend_api::ResponseCode_t obj_pre_open(
    common::backend_api::ObjectClientHandle_t client_handle,
    const char** paths,
    unsigned int num_paths)
{
    // std::cerr << "DEBUG: obj_pre_open called for " << num_paths << " paths" << std::endl;
    try
    {
        if (!client_handle)
        {
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<GCSClient *>(client_handle);
        std::vector<std::string> paths_vec;
        paths_vec.reserve(num_paths);
        for (unsigned int i = 0; i < num_paths; ++i) {
            if (paths[i]) {
                paths_vec.emplace_back(paths[i]);
            }
        }
        ptr->PreOpen(paths_vec);
        return common::ResponseCode::Success;
    }
    catch (...)
    {
        // std::cerr << "DEBUG ERROR: obj_pre_open exception" << std::endl;
        return common::ResponseCode::UnknownError;
    }
}

}; // namespace runai::llm::streamer::impl::gcs