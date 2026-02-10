#include <algorithm>
#include <string>
#include <utility>
#include <optional>
#include <vector>
#include <future>
#include <memory>
#include <functional>
#include <iostream>
#include <cstring>

#include "google/cloud/future.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/oauth2/credentials.h"
#include "google/cloud/common_options.h"
#include "google/cloud/status_or.h"

#include "google/cloud/storage/async/reader.h"
#include "google/cloud/storage/async/token.h"

#include "gcs/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl::gcs
{

GCSClient::GCSClient(const common::backend_api::ObjectClientConfig_t& config) :
    _stop(false),
    _responder(nullptr),
    _chunk_bytesize(config.default_storage_chunk_size)
{
    if (_client_config.use_new_async_client) {
        _new_async_client = std::make_unique<google::cloud::storage::AsyncClient>(_client_config.options);
    } else {
        _client = std::make_unique<AsyncGcsClient>(_client_config.options, _client_config.max_concurrency);
    }
    std::cout << "DEBUG: Custom Run:ai Streamer build is active!" << std::endl;
}

bool GCSClient::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    // TODO: Verify credentials once they are passed in via config.
    return true;
}

common::backend_api::Response GCSClient::async_read_response()
{
    if (_responder == nullptr)
    {
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

common::ResponseCode write_stream_to_buffer(
        google::cloud::storage::ObjectReadStream && stream,
        char * dest_buffer,
        size_t bytesize,
        common::backend_api::ObjectRequestId_t request_id) {
    size_t bytes_received = 0;
    while (stream.read(dest_buffer, bytesize)) {
        bytes_received += stream.gcount();
    }
    stream.Close();
    if (bytes_received != bytesize) {
        LOG(ERROR) << "GCS ReadObject received " << bytes_received << " bytes, but "
                << bytesize << " were requested. This is unexpected." << std::endl;
        return common::ResponseCode::FileAccessError;
    }
    if (stream.bad()) {
        // Note: currently a failure to read any sub range fails the entire read request
        //       a retry mechanism should be added for failed reads
        const auto & err = stream.status();
        LOG(ERROR) << "Failed to download GCS object of request " << request_id << " " << err.code() << ": " << err.message();
        return common::ResponseCode::FileAccessError;
    }

    return common::ResponseCode::Success;
}

namespace {
    void read_loop(google::cloud::storage::AsyncReader reader,
                   google::cloud::storage::AsyncToken token,
                   char* buffer,
                   size_t remaining,
                   common::backend_api::ObjectRequestId_t request_id,
                   std::shared_ptr<common::SharedQueue<common::backend_api::Response>> responder,
                   std::shared_ptr<std::atomic<unsigned>> counter,
                   std::shared_ptr<std::atomic<bool>> is_success) {
        
        reader.Read(std::move(token)).then([reader = std::move(reader), buffer, remaining, request_id, responder, counter, is_success](auto f) mutable {
            auto result = f.get();
            if (!result) {
                LOG(ERROR) << "AsyncReader Read failed for request " << request_id << ": " << result.status().message();
                bool previous = is_success->exchange(false);
                if (previous) {
                    common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                    responder->push(std::move(r));
                }
                return;
            }

            auto& pair = *result;
            auto& payload = pair.first;
            auto& new_token = pair.second;

            size_t bytes_read = 0;
            for (const auto& chunk : payload.contents()) {
                if (bytes_read + chunk.size() > remaining) {
                    LOG(ERROR) << "AsyncReader received more data than requested for request " << request_id;
                    bool previous = is_success->exchange(false);
                    if (previous) {
                        common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                        responder->push(std::move(r));
                    }
                    return;
                }
                std::memcpy(buffer + bytes_read, chunk.data(), chunk.size());
                bytes_read += chunk.size();
            }

            if (new_token.valid()) {
                read_loop(std::move(reader), std::move(new_token), buffer + bytes_read, remaining - bytes_read, request_id, responder, counter, is_success);
            } else {
                 if (remaining != bytes_read) {
                     LOG(ERROR) << "AsyncReader stream finished but incomplete for request " << request_id << ". Expected " << remaining << ", got " << bytes_read;
                     bool previous = is_success->exchange(false);
                     if (previous) {
                         common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                         responder->push(std::move(r));
                     }
                     return;
                 }

                 const auto running = counter->fetch_sub(1);
                 LOG(SPAM) << "Async read request " << request_id << " chunk succeeded - " << running << " running";
                 if (running == 1) {
                     common::backend_api::Response r(request_id, common::ResponseCode::Success);
                     responder->push(std::move(r));
                 }
            }
        });
    }
}

common::ResponseCode GCSClient::async_read(const char* path, common::backend_api::ObjectRange_t range, char* destination_buffer, common::backend_api::ObjectRequestId_t request_id)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    if (_client_config.use_new_async_client) {
        char * buffer_ = destination_buffer;
        size_t size = std::max(1UL, range.length/_chunk_bytesize);
        LOG(SPAM) << "Number of chunks is: " << size;

        auto counter = std::make_shared< std::atomic<unsigned> >(size);
        auto is_success = std::make_shared< std::atomic<bool> >(true);

        const auto uri = common::s3::StorageUri(path);
        std::string bucket_name(uri.bucket);
        std::string path_name(uri.path);

        size_t total_ = range.length;
        size_t offset_ = range.offset;

        DescriptorFuture desc_future;
        {
            std::lock_guard<std::mutex> lock(_descriptors_mutex);
            auto it = _object_descriptors.find(path);
            if (it != _object_descriptors.end()) {
                desc_future = it->second;
            } else {
                 auto fut = _new_async_client->Open(google::cloud::storage::BucketName(bucket_name), path_name);
                 desc_future = fut.then([](auto f) -> std::shared_ptr<google::cloud::storage::ObjectDescriptor> {
                    auto result = f.get();
                    if (!result) return nullptr;
                    return std::make_shared<google::cloud::storage::ObjectDescriptor>(*std::move(result));
                }).share();
                _object_descriptors[path] = desc_future;
            }
        }

        for (unsigned i = 0; i < size && !_stop; ++i)
        {
            size_t bytesize_ = (i == size - 1 ? total_ : _chunk_bytesize);
            
            desc_future.then([dest_buffer = buffer_, responder = _responder, request_id, bytesize_, offset_, counter, is_success](auto f) {
                auto descriptor_ptr = f.get();
                if (!descriptor_ptr) {
                    LOG(ERROR) << "Failed to open object descriptor for request " << request_id;
                    bool previous = is_success->exchange(false);
                    if (previous) {
                        common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                        responder->push(std::move(r));
                    }
                    return;
                }
                
                auto result = descriptor_ptr->Read(offset_, bytesize_);
                read_loop(std::move(result.first), std::move(result.second), dest_buffer, bytesize_, request_id, responder, counter, is_success);
            });

            total_ -= bytesize_;
            offset_ += bytesize_;
            buffer_ += bytesize_;
        }
        return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
    }

    char * buffer_ = destination_buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.length/_chunk_bytesize);
    LOG(SPAM) << "Number of chunks is: " << size;

    // each range is divided into chunks (size is the number of chunks)
    // when all the chunks have been read successfuly the response for that range is pushed to the responder

    auto counter = std::make_shared< std::atomic<unsigned> >(size);
    // success flag for the current range is passed to the client
    auto is_success = std::make_shared< std::atomic<bool> >(true);

    const auto uri = common::s3::StorageUri(path);

    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);

    size_t total_ = range.length;
    size_t offset_ = range.offset;

    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : _chunk_bytesize);

        _client->ReadObjectAsync(bucket_name, path_name, google::cloud::storage::ReadRange(offset_, offset_ + bytesize_)).then(
            [dest_buffer = buffer_, responder = _responder, request_id, bytesize_, counter, is_success](auto f) {
            auto stream = f.get();
            auto response_code = write_stream_to_buffer(std::move(stream), dest_buffer, bytesize_, request_id);
            if (response_code == common::ResponseCode::Success)
            {
                const auto running = counter->fetch_sub(1);
                LOG(SPAM) << "Async read request " << request_id << " succeeded - " << running << " running";
                // send success response only if all the requests have succeeded
                // note that unsuccessful attempts do not update the counter
                if (running == 1)
                {
                    common::backend_api::Response r(request_id, response_code);
                    responder->push(std::move(r));
                }
            }
            else
            {
                // Note: currently a failure to read any sub range fails the entire read request
                //       a retry mechanism should be added for failed reads
                bool previous = is_success->exchange(false);
                // send error response only once
                if (previous)
                {
                    common::backend_api::Response r(request_id, response_code);
                    responder->push(std::move(r));
                }
            }
        });

        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

void GCSClient::stop()
{
    _stop = true;
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

}; // namespace runai::llm::streamer::impl::gcs
