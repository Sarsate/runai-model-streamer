#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <optional>
#include <vector>
#include <future>

#include "gcs/client_configuration/client_configuration.h"
#include "gcs/client/async_gcs_client/async_gcs_client.h"

#include "google/cloud/storage/client.h"
#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/object_descriptor.h"
#include "google/cloud/options.h"

#include "common/backend_api/response/response.h"
#include "common/client_mgr/client_mgr.h"
#include "common/storage_uri/storage_uri.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/shared_queue/shared_queue.h"
#include "common/range/range.h"

#include <map>
#include <mutex>
#include <shared_mutex>
#include <functional>

namespace runai::llm::streamer::impl::gcs
{

struct GCSClient : common::IClient
{
    GCSClient(const common::backend_api::ObjectClientConfig_t& config);
    ~GCSClient();

     // verify that client's credentials have not changed
    bool verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const;

    common::ResponseCode async_read(const char* path, common::backend_api::ObjectRange_t range, char* destination_buffer, common::backend_api::ObjectRequestId_t request_id);

    common::backend_api::Response async_read_response();

    // Stop sending requests to the object store
    // If stopped before all requests for an async_read() call are sent, subsequent request chunks will not be sent.
    void stop();

 private:
    std::atomic<bool> _stop;
    ClientConfiguration _client_config;
    const size_t _chunk_bytesize;
    std::unique_ptr<AsyncGcsClient> _client;
    
    // Shared resources across all GCSClient instances
    static std::unique_ptr<google::cloud::storage_experimental::AsyncClient> _new_async_client;
    static std::shared_timed_mutex _descriptors_mutex;
    static std::map<std::string, std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>> _descriptors;
    static std::map<std::string, std::vector<std::function<void(std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor>)>>> _pending_opens;

    // queue of asynchronous responses
    using Responder = common::SharedQueue<common::backend_api::Response>;
    std::shared_ptr<Responder> _responder;
};

}; //namespace runai::llm::streamer::impl::gcs
