#include "gcs/client/async_gcs_client/async_client_grpc.h"

#include <iostream>
#include <cstring>
#include <thread>

#include "google/cloud/future.h"
#include "google/cloud/storage/client.h"

#include "common/storage_uri/storage_uri.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl::gcs
{

static std::shared_ptr<google::cloud::storage_experimental::AsyncClient> global_client;
static std::once_flag client_init_flag;

std::shared_ptr<google::cloud::storage_experimental::AsyncClient> AsyncClientGrpc::GetClient(const ClientConfiguration& config)
{
    std::call_once(client_init_flag, [&config]() {
        LOG(DEBUG) << "Initializing Global AsyncClient";
        global_client = std::make_shared<google::cloud::storage_experimental::AsyncClient>(config.options);
    });
    return global_client;
}

AsyncClientGrpc::AsyncClientGrpc(const ClientConfiguration& config)
{
    LOG(DEBUG) << "Initializing AsyncClientGrpc instance";
    _client = GetClient(config);
}

AsyncClientGrpc::~AsyncClientGrpc() = default;

void AsyncClientGrpc::Stop()
{
    _stop = true;
}

void AsyncClientGrpc::PreOpen(const std::vector<std::string>& paths)
{
    // No-op. Read() handles opening.
}

void AsyncClientGrpc::StreamToBuffer(
    google::cloud::storage_experimental::AsyncReader reader,
    google::cloud::storage_experimental::AsyncToken token,
    char* buffer,
    size_t remaining_in_chunk,
    common::backend_api::ObjectRequestId_t request_id,
    std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
    std::shared_ptr<std::atomic<unsigned>> pending_chunks,
    std::shared_ptr<std::atomic<bool>> is_success)
{
    reader.Read(std::move(token)).then(
        [this, reader = std::move(reader), buffer, remaining_in_chunk, request_id, responder, pending_chunks, is_success]
        (auto f) mutable {
            if (_stop) return; 

            auto result = f.get();
            if (!result) {
                LOG(ERROR) << "StreamToBuffer failed for request " << request_id << ": " << result.status().message();
                if (is_success->exchange(false)) {
                    responder->push({request_id, common::ResponseCode::FileAccessError});
                }
                return;
            }

            auto& payload = result->first;
            auto& next_token = result->second;
            
            size_t bytes_copied = 0;
            for (const auto& chunk : payload.contents()) {
                if (bytes_copied + chunk.size() > remaining_in_chunk) {
                    LOG(ERROR) << "StreamToBuffer overflow for request " << request_id;
                    if (is_success->exchange(false)) {
                        responder->push({request_id, common::ResponseCode::FileAccessError});
                    }
                    return;
                }
                std::memcpy(buffer + bytes_copied, chunk.data(), chunk.size());
                bytes_copied += chunk.size();
            }

            if (next_token.valid()) {
                StreamToBuffer(std::move(reader), std::move(next_token), 
                               buffer + bytes_copied, remaining_in_chunk - bytes_copied, 
                               request_id, responder, pending_chunks, is_success);
            } else {
                if (bytes_copied != remaining_in_chunk) {
                    LOG(ERROR) << "StreamToBuffer incomplete read for request " << request_id 
                               << ". Expected " << remaining_in_chunk << ", got " << bytes_copied;
                     if (is_success->exchange(false)) {
                        responder->push({request_id, common::ResponseCode::FileAccessError});
                    }
                    return;
                }

                if (pending_chunks->fetch_sub(1) == 1) {
                    LOG(DEBUG) << "Request " << request_id << " completed successfully.";
                    responder->push({request_id, common::ResponseCode::Success});
                }
            }
        });
}

common::ResponseCode AsyncClientGrpc::Read(
    const std::string& path,
    common::backend_api::ObjectRange_t range,
    char* destination_buffer,
    common::backend_api::ObjectRequestId_t request_id,
    std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder)
{
    // We use a single chunk for the whole range in the new async client
    auto pending_chunks = std::make_shared<std::atomic<unsigned>>(1);
    auto is_success = std::make_shared<std::atomic<bool>>(true);

    const auto uri = common::s3::StorageUri(path);
    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);
    std::string key = path; 

    LOG(DEBUG) << "Starting Read for request " << request_id << " range " << range.offset << "-" << range.length;

    // Helper to trigger read on a descriptor
    auto trigger_read = [this, range, destination_buffer, request_id, responder, pending_chunks, is_success](
        std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor) {
            
            LOG(DEBUG) << "Triggering Read on descriptor for request " << request_id;
            auto read_result = descriptor->Read(range.offset, range.length);
            
            StreamToBuffer(std::move(read_result.first), std::move(read_result.second), 
                        destination_buffer, range.length, request_id, responder, pending_chunks, is_success);
    };

    _client->Open(google::cloud::storage_experimental::BucketName(bucket_name), path_name)
        .then([this, key, request_id, trigger_read, responder, is_success](auto f) {
            auto result = f.get();
            std::shared_ptr<google::cloud::storage_experimental::ObjectDescriptor> descriptor;
            
            if (!result) {
                LOG(ERROR) << "Failed to open object descriptor for request " << request_id << ": " << result.status().message();
                if (is_success->exchange(false)) {
                    responder->push({request_id, common::ResponseCode::FileAccessError});
                }
                return;
            } 

            LOG(DEBUG) << "Open successful for request " << request_id;
            descriptor = std::make_shared<google::cloud::storage_experimental::ObjectDescriptor>(*std::move(result));
            trigger_read(descriptor);
        });

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

} // namespace runai::llm::streamer::impl::gcs
