#include "gcs/client_configuration/client_configuration.h"

#include "google/cloud/storage/client.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/common_options.h"

#include "common/exception/exception.h"
#include "common/response_code/response_code.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

#include <fstream>
#include <thread>
#include <algorithm>
#include <cstdlib>
#include <set>
#include <chrono>
#include <iostream>

namespace runai::llm::streamer::impl::gcs
{

ClientConfiguration::ClientConfiguration()
{
    
    // Debug helper to check raw env var
    auto check_env = [](const char* name) {
        const char* val = std::getenv(name);
        if (val) {
        } else {
        }
    };

    check_env("RUNAI_STREAMER_S3_MAX_CONNECTIONS");
    check_env("RUNAI_STREAMER_CONCURRENCY");
    check_env("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS");
    check_env("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT");
    check_env("RUNAI_STREAMER_S3_TRACE");
    check_env("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT");

    unsigned nprocs = std::thread::hardware_concurrency();
    // Use at least 8 threads if hardware_concurrency cannot be computed.
    LOG(SPAM) << "Hardware concurrency detected: " << nprocs;
    unsigned default_max_concurrency = nprocs == 0 ? 8U : 1U;

    const auto max_connections = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_MAX_CONNECTIONS", 0);
    unsigned long worker_concurrency = utils::getenv<unsigned long>("RUNAI_STREAMER_CONCURRENCY", 8UL);

    if (max_connections) {
        max_concurrency = max_connections;
    } else {
        LOG(SPAM) << "Streamer worker concurrency: " << worker_concurrency;
        max_concurrency = std::max(static_cast<unsigned long>(default_max_concurrency), nprocs * 2 / worker_concurrency);
    }
    LOG(DEBUG) << "GCS per-client concurrency is set to: " << max_concurrency;

    // if the transfer speed is less than the low speed limit for request_timeout_ms milliseconds the transfer is aborted and retried
    const auto request_timeout_ms = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS", 600000);
    if (request_timeout_ms)
    {
        LOG(DEBUG) << "GCS request timeout is set to " << request_timeout_ms << " ms";
        std::chrono::seconds request_timeout_seconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::milliseconds(request_timeout_ms));
        options.set<google::cloud::storage::DownloadStallTimeoutOption>(request_timeout_seconds);
    }

    const auto low_speed_limit = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT", 0);
    if (low_speed_limit)
    {
        LOG(DEBUG) << "GCS minimum speed is set to " << low_speed_limit << " bytes in second";
        options.set<google::cloud::storage::DownloadStallMinimumRateOption>(low_speed_limit);
    }

    /*
    const auto trace_gcs = utils::getenv<bool>("RUNAI_STREAMER_S3_TRACE", false);
    if (trace_gcs)
    {
        LOG(DEBUG) << "Enabling log tracing for rpc/auth/http modules for GCS API calls";
        std::set<std::string> logging_components;
        logging_components.insert("rpc");
        logging_components.insert("auth");
        logging_components.insert("http");

        options.set<google::cloud::LoggingComponentsOption>(std::move(logging_components));
    }
    */

    const auto sa_key_file_name = utils::getenv<std::string>("RUNAI_STREAMER_GCS_CREDENTIAL_FILE", "");
    if (!sa_key_file_name.empty()) {
        LOG(DEBUG) << "Loading credentials for Service Account from file: " << sa_key_file_name;
        try {
            // Open the file stream.
            auto is = std::ifstream(sa_key_file_name);

            // Configure the stream to throw an exception on any failure (e.g., file not found, read error).
            is.exceptions(std::ifstream::failbit | std::ifstream::badbit);

            // Read the entire file into the 'contents' string.
            auto contents = std::string(std::istreambuf_iterator<char>(is.rdbuf()), {});

            // Use the contents to set the credentials.
            options.set<google::cloud::UnifiedCredentialsOption>(
                google::cloud::MakeServiceAccountCredentials(contents));
        } catch (const std::ios_base::failure& ex) {
            LOG(ERROR) << "Failed to read service account key file: " << sa_key_file_name;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
    }

    try {
        use_new_async_client = utils::getenv<bool>("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT", false);
    } catch (const std::exception& e) {
        // Fallback or rethrow? Let's check the raw value manually to be helpful.
        std::string raw_val = std::getenv("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT") ? std::getenv("RUNAI_STREAMER_GCS_USE_ASYNC_CLIENT") : "";
        if (raw_val == "true" || raw_val == "True" || raw_val == "TRUE") {
            use_new_async_client = true;
        } else {
             throw; // Rethrow if it's not a simple boolean string mismatch
        }
    }

    if (use_new_async_client) {
        LOG(DEBUG) << "Using new AsyncClient";
        const auto grpc_endpoint = utils::getenv<std::string>("RUNAI_STREAMER_GCS_GRPC_ENDPOINT", "");
        const auto grpc_authority = utils::getenv<std::string>("RUNAI_STREAMER_GCS_GRPC_AUTHORITY", "");

        if (!grpc_endpoint.empty()) {
            LOG(INFO) << "Setting custom gRPC Endpoint: " << grpc_endpoint;
            options.set<google::cloud::EndpointOption>(grpc_endpoint);

            if (!grpc_authority.empty()) {
                options.set<google::cloud::AuthorityOption>(grpc_authority);
            } else {
                options.set<google::cloud::AuthorityOption>(grpc_endpoint);
            }
        }

        // For the new global client (or per-worker client), we want to partition the 
        // global resource budget among the workers.
        // If concurrency is 1, it gets the full budget.
        // If concurrency is 20, each worker gets a slice.
        
        const int kNumGrpcChannels = 12;
        options.set<google::cloud::GrpcNumChannelsOption>(kNumGrpcChannels);

        // thread pool size should be nproc / concurrency
        // Handle nprocs=0 case (unknown)
        unsigned safe_nprocs = nprocs == 0 ? 8 : nprocs;
        int num_threads = std::max(1UL, safe_nprocs / worker_concurrency);
        
        options.set<google::cloud::GrpcBackgroundThreadPoolSizeOption>(num_threads);

        LOG(DEBUG) << "Setting GrpcNumChannelsOption to " << kNumGrpcChannels 
                   << " and GrpcBackgroundThreadPoolSizeOption to " << num_threads
                   << " (Worker Concurrency: " << worker_concurrency << ")";
    }
}

}; // namespace runai::llm::streamer::impl::gcs
