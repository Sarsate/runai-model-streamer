load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def load_gcp_repo():
    # Fetch the Google Cloud C++ libraries.
    # NOTE: Update this version and SHA256 as needed.
    http_archive(
        name = "google_cloud_cpp",
        sha256 = "492734e092e5150d8395797f0d269f3d1e49ba3a959db4a332d15a1f382ff7ee",
        strip_prefix = "google-cloud-cpp-2.46.0",
	url = "https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/v2.46.0.tar.gz"
    )
