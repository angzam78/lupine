# dedup.cmake — Build configuration for the dedup feature.
# Included from the top-level CMakeLists.txt via:
#   include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/dedup.cmake)
#
# This file adds:
# - xxHash static library (vendored, for XXH3_128bits content hashing)
# - dedup.cpp and dedup_s3.cpp to all source lists
# - dedup.h and dedup_s3.h to all header lists
# - OpenSSL + libcurl linking on all targets (for S3 L2 TLS + SigV4 + HTTP)
# - xxhash include directory

# xxHash is vendored (third_party/xxhash, BSD-2-Clause) for the dedup
# cache's 128-bit content hashing (XXH3_128bits).
add_library(lupine_xxhash STATIC ${CMAKE_CURRENT_SOURCE_DIR}/third_party/xxhash/xxhash.c)
set_target_properties(lupine_xxhash PROPERTIES POSITION_INDEPENDENT_CODE ON)
if(NOT MSVC AND NOT CMAKE_BUILD_TYPE)
  target_compile_options(lupine_xxhash PRIVATE -O2)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/xxhash)

# OpenSSL: needed for S3 L2 (TLS to S3-compatible storage, SigV4 signing)
# and for https:// client connections to Lupine servers.
find_package(OpenSSL QUIET)

# libcurl: S3 L2 HTTP transport (handles both HTTP/1.1 and HTTP/2 via ALPN)
find_package(CURL REQUIRED)

# Dedup sources added to all targets
list(APPEND CLIENT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup_s3.cpp
)
list(APPEND SERVER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup_s3.cpp
)
list(APPEND NVML_CLIENT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup_s3.cpp
)
list(APPEND CLIENT_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup_s3.h
)
list(APPEND SERVER_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup.h
    ${CMAKE_CURRENT_SOURCE_DIR}/dedup_s3.h
)

# Dedup libraries to link on all targets
set(DEDUP_LIBS lupine_xxhash CURL::libcurl)
if(OpenSSL_FOUND)
    list(APPEND DEDUP_LIBS OpenSSL::SSL OpenSSL::Crypto)
endif()
