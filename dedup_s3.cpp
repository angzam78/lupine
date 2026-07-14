// S3-compatible L2 cache for the dedup system. See dedup_s3.h and
// docs/dedup-architecture.md.
//
// This file implements a minimal S3 client (HTTP/1.1 + TLS via OpenSSL,
// AWS SigV4 signing) and the L2 cache logic: manifest management, async
// write-through, L2 lookup with L1 promotion, and cleanup.
//
// The S3 client is intentionally minimal — no SDK dependency, no streaming
// uploads, no multipart. Each operation is a single HTTP request. Chunk
// sizes are bounded by LUPINE_COMPRESS_BLOCK_BYTES (4 MiB), well within
// S3's single-PUT limit (5 GiB).

#include "dedup_s3.h"
#include "dedup.h"
#include "lupine_log.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <condition_variable>
#include <curl/curl.h>
#include <deque>
#include <lz4.h>
#include <map>
#include <mutex>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

namespace {

struct s3_config {
  std::string endpoint;       // e.g. "s3.us-west-004.backblazeb2.com" or "cache-worker.example.com"
  std::string bucket;
  std::string region;
  std::string access_key;
  std::string secret_key;
  std::string prefix;         // optional key prefix within the bucket
  bool use_tls = true;        // true if endpoint starts with https://
  bool path_style = true;     // B2/MinIO use path-style; AWS S3 uses virtual-host-style
  size_t cache_bytes = 0;     // L2 cache cap (LUPINE_DEDUP_S3_CACHE_BYTES)
};

const s3_config &get_s3_config() {
  static s3_config cfg = [] {
    s3_config c;
    const char *ep = std::getenv("LUPINE_DEDUP_S3_ENDPOINT");
    const char *bk = std::getenv("LUPINE_DEDUP_S3_BUCKET");
    const char *rg = std::getenv("LUPINE_DEDUP_S3_REGION");
    const char *ak = std::getenv("LUPINE_DEDUP_S3_ACCESS_KEY");
    const char *sk = std::getenv("LUPINE_DEDUP_S3_SECRET_KEY");
    const char *pf = std::getenv("LUPINE_DEDUP_S3_PREFIX");
    const char *cb = std::getenv("LUPINE_DEDUP_S3_CACHE_BYTES");
    const char *ps = std::getenv("LUPINE_DEDUP_S3_PATH_STYLE");

    if (ep && *ep) {
      c.endpoint = ep;
      if (c.endpoint.find("https://") == 0) {
        c.use_tls = true;
        c.endpoint = c.endpoint.substr(8);
      } else if (c.endpoint.find("http://") == 0) {
        c.use_tls = false;
        c.endpoint = c.endpoint.substr(7);
      }
    }
    if (bk) c.bucket = bk;
    if (rg) c.region = rg;
    if (ak) c.access_key = ak;
    if (sk) c.secret_key = sk;
    if (pf) c.prefix = pf;
    if (cb) {
      char *end = nullptr;
      unsigned long long v = std::strtoull(cb, &end, 10);
      if (end != cb && v > 0) c.cache_bytes = static_cast<size_t>(v);
    }
    c.path_style = !(ps && std::strcmp(ps, "0") == 0);
    return c;
  }();
  return cfg;
}

} // namespace

int lupine_dedup_s3_configured() {
  static int cached = [] {
    if (!lupine_dedup_enabled_globally()) return 0;
    const char *bk = std::getenv("LUPINE_DEDUP_S3_BUCKET");
    return (bk != nullptr && *bk != '\0') ? 1 : 0;
  }();
  return cached;
}

// ---------------------------------------------------------------------------
// Crypto helpers (SHA-256, HMAC-SHA256, hex encoding)
// ---------------------------------------------------------------------------

namespace {

void sha256(const void *data, size_t len, unsigned char out[32]) {
  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
  EVP_DigestUpdate(ctx, data, len);
  unsigned int outlen = 0;
  EVP_DigestFinal_ex(ctx, out, &outlen);
  EVP_MD_CTX_free(ctx);
}

void sha256_hex(const void *data, size_t len, char out[65]) {
  unsigned char raw[32];
  sha256(data, len, raw);
  static const char hex[] = "0123456789abcdef";
  for (int i = 0; i < 32; ++i) {
    out[i * 2] = hex[raw[i] >> 4];
    out[i * 2 + 1] = hex[raw[i] & 0xf];
  }
  out[64] = '\0';
}

void hmac_sha256(const void *key, size_t key_len,
                 const void *data, size_t data_len,
                 unsigned char out[32]) {
  unsigned int outlen = 0;
  HMAC(EVP_sha256(), key, static_cast<int>(key_len),
       static_cast<const unsigned char *>(data), data_len,
       out, &outlen);
}

std::string hmac_sha256_hex(const std::string &key, const std::string &data) {
  unsigned char raw[32];
  hmac_sha256(key.data(), key.size(), data.data(), data.size(), raw);
  static const char hex[] = "0123456789abcdef";
  std::string out(64, '\0');
  for (int i = 0; i < 32; ++i) {
    out[i * 2] = hex[raw[i] >> 4];
    out[i * 2 + 1] = hex[raw[i] & 0xf];
  }
  return out;
}

// URI-encodes a string per RFC 3986. S3 SigV4 requires '/' to be encoded
// in the canonical URI (unlike general URI encoding which leaves '/' alone).
std::string uri_encode(const std::string &s, bool encode_slash) {
  static const char hex[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(s.size() * 3);
  for (char c : s) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~') {
      out += c;
    } else if (c == '/' && !encode_slash) {
      out += c;
    } else {
      out += '%';
      out += hex[(static_cast<unsigned char>(c) >> 4) & 0xf];
      out += hex[static_cast<unsigned char>(c) & 0xf];
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// HTTP transport via libcurl
//
// libcurl handles HTTP/1.1 and HTTP/2 transparently (ALPN negotiation),
// connection reuse (keep-alive), chunked transfer encoding, TLS, and
// response parsing. This replaces both the hand-rolled HTTP/1.1 client
// and the nghttp2 HTTP/2 client — ~450 lines of transport code replaced
// with ~120 lines of curl_easy calls.
//
// Each worker thread gets its own thread-local CURL* handle with a
// persistent connection, enabling parallel S3 uploads without multiplexing
// complexity.
// ---------------------------------------------------------------------------

struct s3_response {
  int status = 0;
  std::map<std::string, std::string> headers;
  std::vector<unsigned char> body;
};

// libcurl write callback: appends data to the response body
static size_t s3_curl_write_cb(char *ptr, size_t size, size_t nmemb,
                                void *userdata) {
  auto *resp = static_cast<s3_response *>(userdata);
  size_t total = size * nmemb;
  resp->body.insert(resp->body.end(), ptr, ptr + total);
  return total;
}

// libcurl header callback: captures status code and headers
static size_t s3_curl_header_cb(char *buffer, size_t size, size_t nitems,
                                 void *userdata) {
  auto *resp = static_cast<s3_response *>(userdata);
  size_t total = size * nitems;
  std::string line(buffer, total);
  // Remove trailing \r\n
  while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
    line.pop_back();

  // Status line: "HTTP/1.1 200 OK" or "HTTP/2 200"
  if (line.substr(0, 5) == "HTTP/") {
    auto sp = line.find(' ');
    if (sp != std::string::npos) {
      auto sp2 = line.find(' ', sp + 1);
      std::string code = (sp2 == std::string::npos)
          ? line.substr(sp + 1) : line.substr(sp + 1, sp2 - sp - 1);
      resp->status = std::atoi(code.c_str());
    }
    return total;
  }

  // Regular header
  auto colon = line.find(':');
  if (colon != std::string::npos) {
    std::string key = line.substr(0, colon);
    std::string val = line.substr(colon + 1);
    while (!val.empty() && val[0] == ' ') val = val.substr(1);
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);
    resp->headers[key] = val;
  }
  return total;
}

// Thread-local CURL handle — each worker thread gets its own persistent
// connection. libcurl reuses the connection across requests (keep-alive
// for HTTP/1.1, multiplexing for HTTP/2).
static thread_local CURL *tls_curl = nullptr;

static CURL *get_curl_handle() {
  if (tls_curl) return tls_curl;
  tls_curl = curl_easy_init();
  if (tls_curl) {
    // Enable HTTP/2 if available (negotiated via ALPN)
    curl_easy_setopt(tls_curl, CURLOPT_HTTP_VERSION,
                     CURL_HTTP_VERSION_2_0);
    // 30s timeout
    curl_easy_setopt(tls_curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(tls_curl, CURLOPT_CONNECTTIMEOUT, 10L);
    // Verify TLS
    curl_easy_setopt(tls_curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(tls_curl, CURLOPT_SSL_VERIFYHOST, 2L);
    // Set callbacks
    curl_easy_setopt(tls_curl, CURLOPT_WRITEFUNCTION, s3_curl_write_cb);
    curl_easy_setopt(tls_curl, CURLOPT_HEADERFUNCTION, s3_curl_header_cb);
  }
  return tls_curl;
}

// Build the full URL for an S3 request
static std::string s3_build_url(const s3_config &cfg,
                                 const std::string &host,
                                 const std::string &http_path) {
  std::string scheme = cfg.use_tls ? "https" : "http";
  return scheme + "://" + host + http_path;
}

// Perform a signed S3 request via libcurl. The signed headers from s3_sign
// are passed as CURL headers. libcurl handles connection reuse, HTTP
// version negotiation, and response parsing.
static bool s3_curl_request(const s3_config &cfg,
                             const std::string &method,
                             const std::string &host,
                             const std::string &http_path,
                             const std::vector<std::pair<std::string, std::string>> &signed_headers,
                             const void *body, size_t body_size,
                             s3_response &resp) {
  CURL *curl = get_curl_handle();
  if (!curl) return false;

  // Reset handle for a new request (keeps connection pool)
  curl_easy_reset(curl);
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, s3_curl_write_cb);
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, s3_curl_header_cb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, &resp);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());

  // Build URL
  std::string url = s3_build_url(cfg, host, http_path);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

  // Build headers list from signed headers
  struct curl_slist *hdrs = nullptr;
  for (auto &h : signed_headers) {
    std::string hdr = h.first + ": " + h.second;
    hdrs = curl_slist_append(hdrs, hdr.c_str());
  }
  // Content-Length is needed for PUT/POST with body
  if (body_size > 0) {
    std::string cl = "Content-Length: " + std::to_string(body_size);
    hdrs = curl_slist_append(hdrs, cl.c_str());
  }
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);

  // Set body for PUT/POST
  if (body && body_size > 0) {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body_size);
  }

  // Perform the request
  CURLcode rv = curl_easy_perform(curl);
  curl_slist_free_all(hdrs);

  if (rv != CURLE_OK) {
    LUPINE_LOG_ERROR("curl_easy_perform failed: " +
                     std::string(curl_easy_strerror(rv)));
    return false;
  }

  // If we didn't get a status code, something went wrong
  if (resp.status == 0) return false;

  return true;
}

// ---------------------------------------------------------------------------
// AWS SigV4 signing
// ---------------------------------------------------------------------------

struct signed_request {
  std::vector<std::pair<std::string, std::string>> headers;
  std::string method;
  std::string host;
  std::string path;         // URL-encoded path
  std::string query;        // canonical query string
};

// Builds the SigV4-signed headers for an S3 request.
signed_request s3_sign(
    const std::string &method,
    const std::string &host,
    const std::string &path,          // already URL-encoded
    const std::string &query_string,  // already URL-encoded, or empty
    const std::string &payload_hash,  // hex SHA-256 of body, or "UNSIGNED-PAYLOAD"
    const void *body, size_t body_size,
    const s3_config &cfg) {

  // Amz date format: "20260715T120000Z"
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_utc = *gmtime(&time_t_now);
  char date_str[16], datetime_str[32];
  std::strftime(date_str, sizeof(date_str), "%Y%m%d", &tm_utc);
  std::strftime(datetime_str, sizeof(datetime_str), "%Y%m%dT%H%M%SZ", &tm_utc);

  std::string amz_date = datetime_str;
  std::string date_stamp = date_str;

  // Build canonical headers (must be sorted by key, lowercase).
  // B2 requires x-amz-content-sha256 to be included in the signature.
  std::map<std::string, std::string> hdrs;
  hdrs["host"] = host;
  hdrs["x-amz-content-sha256"] = payload_hash;
  hdrs["x-amz-date"] = amz_date;

  std::string canonical_headers;
  std::string signed_headers;
  for (auto &h : hdrs) {
    canonical_headers += h.first + ":" + h.second + "\n";
    if (!signed_headers.empty()) signed_headers += ";";
    signed_headers += h.first;
  }

  // Canonical request
  std::string canonical_request = method + "\n";
  canonical_request += path + "\n";
  canonical_request += query_string + "\n";
  canonical_request += canonical_headers + "\n";
  canonical_request += signed_headers + "\n";
  canonical_request += payload_hash;

  // String to sign
  std::string credential_scope = date_stamp + "/" + cfg.region + "/s3/aws4_request";
  char hashed_cr[65];
  sha256_hex(canonical_request.data(), canonical_request.size(), hashed_cr);

  std::string string_to_sign = "AWS4-HMAC-SHA256\n";
  string_to_sign += amz_date + "\n";
  string_to_sign += credential_scope + "\n";
  string_to_sign += hashed_cr;

  // Signing key: HMAC chain
  // k_date = HMAC("AWS4" + secret_key, date_stamp)
  // k_region = HMAC(k_date, region)
  // k_service = HMAC(k_region, "s3")
  // k_signing = HMAC(k_service, "aws4_request")
  std::string k_secret = "AWS4" + cfg.secret_key;
  unsigned char k_date[32], k_region[32], k_service[32], k_signing[32];
  hmac_sha256(k_secret.data(), k_secret.size(),
              date_stamp.data(), date_stamp.size(), k_date);
  hmac_sha256(k_date, 32, cfg.region.data(), cfg.region.size(), k_region);
  hmac_sha256(k_region, 32, "s3", 2, k_service);
  hmac_sha256(k_service, 32, "aws4_request", 12, k_signing);

  // Signature
  unsigned char signature[32];
  hmac_sha256(k_signing, 32, string_to_sign.data(), string_to_sign.size(), signature);
  static const char hex[] = "0123456789abcdef";
  std::string sig_hex(64, '\0');
  for (int i = 0; i < 32; ++i) {
    sig_hex[i * 2] = hex[signature[i] >> 4];
    sig_hex[i * 2 + 1] = hex[signature[i] & 0xf];
  }

  // Authorization header
  std::string auth = "AWS4-HMAC-SHA256 Credential=" + cfg.access_key + "/" +
                     credential_scope + ", SignedHeaders=" + signed_headers +
                     ", Signature=" + sig_hex;

  signed_request sr;
  sr.method = method;
  sr.host = host;
  sr.path = path;
  sr.query = query_string;
  for (auto &h : hdrs) {
    sr.headers.push_back({h.first, h.second});
  }
  sr.headers.push_back({"authorization", auth});
  // Note: Content-Length is NOT added — nghttp2 handles this via DATA frame
  // framing. Adding it as a separate header can cause HTTP/2 protocol errors.
  return sr;
}

// ---------------------------------------------------------------------------
// S3 operations (using nghttp2 HTTP/2 transport)
// ---------------------------------------------------------------------------

// Builds the host and path for an S3 request based on path_style config.
void s3_build_host_path(const s3_config &cfg, const std::string &key,
                        std::string &out_host, std::string &out_path) {
  std::string full_key = cfg.prefix.empty() ? key : (cfg.prefix + "/" + key);
  if (cfg.path_style) {
    out_host = cfg.endpoint;
    out_path = "/" + uri_encode(cfg.bucket, true) + "/" + uri_encode(full_key, false);
  } else {
    out_host = cfg.bucket + "." + cfg.endpoint;
    out_path = "/" + uri_encode(full_key, false);
  }
}

// Performs a signed S3 request via HTTP/2 and returns the response.
// Uses the persistent nghttp2 session (one TLS+H2 connection per child).
bool s3_do_request(const s3_config &cfg, const std::string &method,
                   const std::string &key, const std::string &query,
                   const void *body, size_t body_size,
                   s3_response &resp) {
  std::string host, path;
  s3_build_host_path(cfg, key, host, path);

  // HTTP/2 path includes the query string (nghttp2 sends it as :path)
  std::string http_path = path;
  if (!query.empty()) http_path += "?" + query;

  // Payload hash for SigV4
  char hash_hex[65];
  if (body_size > 0) {
    sha256_hex(body, body_size, hash_hex);
  } else {
    std::memcpy(hash_hex,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 65);
  }

  // Sign the request. s3_sign takes canonical_path (no query) and
  // canonical_query separately for SigV4. The HTTP :path includes both.
  signed_request sr = s3_sign(method, host, path, query, hash_hex,
                               body, body_size, cfg);

  // Strip port from host for HTTP/2 :authority (nghttp2/HTTP-2 spec:
  // authority should not include the port for default-port connections)
  std::string authority = host;
  auto colon = authority.find(':');
  if (colon != std::string::npos) {
    authority = authority.substr(0, colon);
  }

  return s3_curl_request(cfg, method, authority, http_path,
                        sr.headers, body, body_size, resp);
}

// PUT: upload a chunk to S3.
bool s3_put_object(const s3_config &cfg, const std::string &key,
                   const void *data, size_t size) {
  s3_response resp;
  if (!s3_do_request(cfg, "PUT", key, "", data, size, resp)) return false;
  return resp.status >= 200 && resp.status < 300;
}

// GET: download a chunk from S3.
bool s3_get_object(const s3_config &cfg, const std::string &key,
                   std::vector<unsigned char> &out) {
  s3_response resp;
  if (!s3_do_request(cfg, "GET", key, "", nullptr, 0, resp)) return false;
  if (resp.status != 200) return false;
  out = std::move(resp.body);
  return true;
}

// HEAD: check if an object exists (returns status code).
bool s3_head_object(const s3_config &cfg, const std::string &key, int *status = nullptr) {
  s3_response resp;
  if (!s3_do_request(cfg, "HEAD", key, "", nullptr, 0, resp)) return false;
  if (status) *status = resp.status;
  return resp.status == 200 || resp.status == 404;
}

// DeleteObjects: batch delete up to 1000 objects.
bool s3_delete_objects(const s3_config &cfg,
                       const std::vector<std::string> &keys) {
  if (keys.empty()) return true;
  // Build XML body for DeleteObjects
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete>";
  for (auto &k : keys) {
    xml += "<Object><Key>" + k + "</Key></Object>";
  }
  xml += "</Delete>";

  // For DeleteObjects, the request goes to the bucket path with ?delete=
  std::string host, path;
  if (cfg.path_style) {
    host = cfg.endpoint;
    path = "/" + uri_encode(cfg.bucket, true);
  } else {
    host = cfg.bucket + "." + cfg.endpoint;
    path = "/";
  }
  std::string query = "delete=";
  std::string http_path = path + "?" + query;

  char hash_hex[65];
  sha256_hex(xml.data(), xml.size(), hash_hex);

  signed_request sr = s3_sign("POST", host, path, query,
                               hash_hex, xml.data(), xml.size(), cfg);
  sr.headers.push_back({"content-type", "application/xml"});  // lowercase for HTTP/2

  // Strip port from host
  std::string authority = host;
  auto colon = authority.find(':');
  if (colon != std::string::npos) authority = authority.substr(0, colon);

  s3_response resp;
  return s3_curl_request(cfg, "POST", authority, http_path,
                        sr.headers, xml.data(), xml.size(), resp) &&
         resp.status >= 200 && resp.status < 300;
}

// ListObjectsV2: list objects in the bucket (used for initial manifest
// creation if no manifest exists).
struct s3_list_entry {
  std::string key;
  std::string last_modified;  // ISO 8601
  size_t size;
};

bool s3_list_objects(const s3_config &cfg, const std::string &continuation,
                     std::vector<s3_list_entry> &out_entries,
                     bool &out_truncated, std::string &out_next_continuation) {
  std::string query = "list-type=2&max-keys=1000";
  if (!continuation.empty()) {
    query += "&continuation-token=" + uri_encode(continuation, true);
  }

  std::string host, path;
  if (cfg.path_style) {
    host = cfg.endpoint;
    path = "/" + uri_encode(cfg.bucket, true);
  } else {
    host = cfg.bucket + "." + cfg.endpoint;
    path = "/";
  }
  std::string http_path = path + "?" + query;

  char hash_hex[65];
  std::memcpy(hash_hex,
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 65);

  signed_request sr = s3_sign("GET", host, path, query, hash_hex,
                               nullptr, 0, cfg);

  // Strip port from host
  std::string authority = host;
  auto colon = authority.find(':');
  if (colon != std::string::npos) authority = authority.substr(0, colon);

  s3_response resp;
  if (!s3_curl_request(cfg, "GET", authority, http_path,
                      sr.headers, nullptr, 0, resp)) return false;
  if (resp.status != 200) return false;

  // Parse XML (minimal parser — just look for <Key>, <Size>, <LastModified>,
  // <IsTruncated>, <NextContinuationToken>)
  std::string xml(resp.body.begin(), resp.body.end());
  out_truncated = (xml.find("<IsTruncated>true</IsTruncated>") != std::string::npos);

  // Extract NextContinuationToken
  {
    auto start = xml.find("<NextContinuationToken>");
    if (start != std::string::npos) {
      start += std::string("<NextContinuationToken>").size();
      auto end = xml.find("</NextContinuationToken>", start);
      if (end != std::string::npos) {
        out_next_continuation = xml.substr(start, end - start);
      }
    }
  }

  // Extract entries
  size_t pos = 0;
  while ((pos = xml.find("<Contents>", pos)) != std::string::npos) {
    s3_list_entry e;
    auto end = xml.find("</Contents>", pos);
    if (end == std::string::npos) break;
    std::string block = xml.substr(pos, end - pos);

    auto k_start = block.find("<Key>");
    if (k_start != std::string::npos) {
      k_start += 5;
      auto k_end = block.find("</Key>", k_start);
      if (k_end != std::string::npos) e.key = block.substr(k_start, k_end - k_start);
    }
    auto s_start = block.find("<Size>");
    if (s_start != std::string::npos) {
      s_start += 6;
      auto s_end = block.find("</Size>", s_start);
      if (s_end != std::string::npos) {
        e.size = std::stoul(block.substr(s_start, s_end - s_start));
      }
    }
    auto m_start = block.find("<LastModified>");
    if (m_start != std::string::npos) {
      m_start += 14;
      auto m_end = block.find("</LastModified>", m_start);
      if (m_end != std::string::npos) {
        e.last_modified = block.substr(m_start, m_end - m_start);
      }
    }
    if (!e.key.empty()) out_entries.push_back(e);
    pos = end + 11;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Manifest management
// ---------------------------------------------------------------------------

// Manifest entry: hash + timestamp + compressed size.
struct manifest_entry {
  lupine_dedup_hash128 hash;
  uint64_t timestamp;  // Unix epoch seconds
  uint64_t size;       // compressed size on S3
};

// Manifest is stored as a single S3 object: manifest.bin
// Format: [u64 version][u32 count][count × (16 + 8 + 8) bytes]
// Version is a monotonic counter incremented on each rewrite.
struct s3_manifest {
  std::mutex mutex;
  std::vector<manifest_entry> entries;
  uint64_t version = 0;
  bool dirty = false;
  bool loaded = false;

  // Additions that haven't been drained by the additions channel yet.
  // These are hashes newly added to S3 since the last drain.
  std::vector<lupine_dedup_hash128> new_additions;
};

s3_manifest &get_manifest() {
  static s3_manifest m;
  return m;
}

std::string manifest_key() {
  const s3_config &cfg = get_s3_config();
  return cfg.prefix.empty() ? "manifest.bin" : (cfg.prefix + "/manifest.bin");
}

// Hash to hex string for S3 key.
std::string hash_to_s3_key(const lupine_dedup_hash128 &h) {
  char hex[33];
  for (int i = 0; i < 16; ++i) {
    std::snprintf(hex + i * 2, 3, "%02x", h.bytes[i]);
  }
  const s3_config &cfg = get_s3_config();
  // Shard by first 2 hex chars (same as local disk).
  std::string shard(hex, 2);
  std::string rest(hex + 2);
  std::string key = "chunks/" + shard + "/" + rest;
  if (!cfg.prefix.empty()) key = cfg.prefix + "/" + key;
  return key;
}

// S3 key to hash (reverse).
bool s3_key_to_hash(const std::string &key, lupine_dedup_hash128 *out) {
  // Key format: [prefix/]chunks/ab/cdef0123...
  size_t pos = key.find("chunks/");
  if (pos == std::string::npos) return false;
  std::string hash_part = key.substr(pos + 7);  // after "chunks/"
  // hash_part is "ab/cdef0123..."
  if (hash_part.size() < 33) return false;  // 2 + 1 + 30 = 33
  char hex[33];
  hex[0] = hash_part[0];
  hex[1] = hash_part[1];
  std::memcpy(hex + 2, hash_part.c_str() + 3, 30);
  hex[32] = '\0';
  for (int i = 0; i < 16; ++i) {
    unsigned int byte;
    if (std::sscanf(hex + i * 2, "%02x", &byte) != 1) return false;
    out->bytes[i] = static_cast<uint8_t>(byte);
  }
  return true;
}

void load_manifest_from_s3() {
  s3_manifest &m = get_manifest();
  std::lock_guard<std::mutex> lock(m.mutex);
  m.entries.clear();
  m.version = 0;
  m.new_additions.clear();

  const s3_config &cfg = get_s3_config();
  std::vector<unsigned char> data;
  if (!s3_get_object(cfg, manifest_key(), data)) {
    // No manifest yet — try to build from ListObjectsV2
    LUPINE_LOG_DEBUG("S3 manifest not found, scanning bucket...");
    std::string continuation;
    bool truncated = true;
    while (truncated) {
      std::vector<s3_list_entry> entries;
      std::string next_token;
      if (!s3_list_objects(cfg, continuation, entries, truncated, next_token)) {
        LUPINE_LOG_ERROR("S3 list_objects failed during manifest build");
        m.loaded = true;
        m.dirty = true;  // will try to write on next flush
        return;
      }
      for (auto &e : entries) {
        lupine_dedup_hash128 h;
        if (s3_key_to_hash(e.key, &h)) {
          manifest_entry me;
          me.hash = h;
          me.size = e.size;
          // Parse ISO 8601 to Unix timestamp (simplified: just use 0 if parse fails)
          me.timestamp = 0;
          m.entries.push_back(me);
        }
      }
      continuation = next_token;
    }
    m.version = 1;
    m.dirty = true;
    m.loaded = true;
    LUPINE_LOG_DEBUG("S3 manifest built from bucket scan: " +
                     std::to_string(m.entries.size()) + " entries");
    return;
  }

  // Parse manifest binary
  if (data.size() < 12) { m.loaded = true; return; }
  size_t offset = 0;
  std::memcpy(&m.version, data.data() + offset, 8); offset += 8;
  uint32_t count = 0;
  std::memcpy(&count, data.data() + offset, 4); offset += 4;
  m.entries.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (offset + 32 > data.size()) break;
    manifest_entry e;
    std::memcpy(e.hash.bytes, data.data() + offset, 16); offset += 16;
    std::memcpy(&e.timestamp, data.data() + offset, 8); offset += 8;
    std::memcpy(&e.size, data.data() + offset, 8); offset += 8;
    m.entries.push_back(e);
  }
  m.loaded = true;
  LUPINE_LOG_DEBUG("S3 manifest loaded: " + std::to_string(m.entries.size()) +
                   " entries, version " + std::to_string(m.version));

  // If the manifest has 0 entries, the bucket might still have objects
  // (e.g., a previous server was killed before writing the manifest).
  // Fall back to scanning the bucket.
  if (m.entries.empty()) {
    LUPINE_LOG_DEBUG("S3 manifest is empty, scanning bucket...");
    std::string continuation;
    bool truncated = true;
    while (truncated) {
      std::vector<s3_list_entry> entries;
      std::string next_token;
      if (!s3_list_objects(cfg, continuation, entries, truncated, next_token)) {
        LUPINE_LOG_ERROR("S3 list_objects failed during manifest rebuild");
        break;
      }
      for (auto &e : entries) {
        lupine_dedup_hash128 h;
        if (s3_key_to_hash(e.key, &h)) {
          manifest_entry me;
          me.hash = h;
          me.size = e.size;
          me.timestamp = 0;
          m.entries.push_back(me);
        }
      }
      continuation = next_token;
    }
    m.dirty = true;
    LUPINE_LOG_DEBUG("S3 manifest rebuilt from bucket scan: " +
                     std::to_string(m.entries.size()) + " entries");
  }
}

void write_manifest_to_s3() {
  s3_manifest &m = get_manifest();
  const s3_config &cfg = get_s3_config();

  std::vector<unsigned char> data;
  data.reserve(12 + m.entries.size() * 32);
  uint64_t version = m.version + 1;
  uint32_t count = static_cast<uint32_t>(m.entries.size());
  data.resize(12);
  std::memcpy(&data[0], &version, 8);
  std::memcpy(&data[8], &count, 4);
  for (auto &e : m.entries) {
    data.insert(data.end(), e.hash.bytes, e.hash.bytes + 16);
    data.insert(data.end(), reinterpret_cast<unsigned char *>(&e.timestamp),
               reinterpret_cast<unsigned char *>(&e.timestamp) + 8);
    data.insert(data.end(), reinterpret_cast<unsigned char *>(&e.size),
               reinterpret_cast<unsigned char *>(&e.size) + 8);
  }

  if (s3_put_object(cfg, manifest_key(), data.data(), data.size())) {
    m.version = version;
    m.dirty = false;
    LUPINE_LOG_DEBUG("S3 manifest written: " + std::to_string(count) +
                     " entries, version " + std::to_string(version));
  } else {
    LUPINE_LOG_ERROR("S3 manifest write failed");
  }
}

// ---------------------------------------------------------------------------
// Availability management
// ---------------------------------------------------------------------------

std::atomic<int> g_s3_available{0};
std::atomic<int> g_s3_test_in_progress{0};

bool test_s3_connection() {
  const s3_config &cfg = get_s3_config();
  // Try to HEAD the manifest object. If it doesn't exist (404), that's fine —
  // S3 is reachable, just empty. Any 2xx or 404 means S3 is available.
  int status = 0;
  if (!s3_head_object(cfg, manifest_key(), &status)) {
    return false;
  }
  return status == 200 || status == 404;
}

void availability_test_loop() {
  for (;;) {
    bool ok = test_s3_connection();
    if (ok) {
      g_s3_available.store(1);
    } else {
      g_s3_available.store(0);
      LUPINE_LOG_ERROR("S3 availability test failed, retrying in 30s");
    }

    // Sleep 30 seconds, but check for shutdown via a flag.
    // For simplicity, we just sleep and let the thread be detached.
    // It will be killed when the child process exits.
    std::this_thread::sleep_for(std::chrono::seconds(30));

    if (!lupine_dedup_s3_configured()) break;
  }
}

// ---------------------------------------------------------------------------
// Async write-through queue
// ---------------------------------------------------------------------------

struct write_queue_entry {
  lupine_dedup_hash128 hash;
  std::vector<unsigned char> data;  // compressed bytes (or raw if no compression)
};

struct write_queue {
  std::mutex mutex;
  std::condition_variable cv;
  std::deque<write_queue_entry> entries;
  bool shutdown = false;
  static constexpr size_t MAX_ENTRIES = 512;  // 512 × 4 MiB = 2 GiB max
  std::atomic<int> put_counter{0};  // shared across workers for manifest flush
};

write_queue &get_write_queue() {
  static write_queue q;
  return q;
}

// Number of parallel S3 upload workers. Each worker has its own persistent
// HTTP/2 connection, so N workers = N concurrent S3 PUTs.
constexpr int S3_WRITE_WORKER_COUNT = 8;

void write_through_worker() {
  for (;;) {
    write_queue_entry entry;
    {
      std::unique_lock<std::mutex> lock(get_write_queue().mutex);
      get_write_queue().cv.wait(lock, [] {
        return get_write_queue().shutdown || !get_write_queue().entries.empty();
      });
      if (get_write_queue().shutdown && get_write_queue().entries.empty()) break;
      if (get_write_queue().entries.empty()) continue;
      entry = std::move(get_write_queue().entries.front());
      get_write_queue().entries.pop_front();
    }

    const s3_config &cfg = get_s3_config();
    std::string key = hash_to_s3_key(entry.hash);
    if (!s3_put_object(cfg, key, entry.data.data(), entry.data.size())) {
      LUPINE_LOG_ERROR("S3 PUT failed for chunk");
      continue;
    }

    // Update manifest
    bool need_manifest_write = false;
    {
      s3_manifest &m = get_manifest();
      std::lock_guard<std::mutex> lock(m.mutex);
      auto now = std::chrono::system_clock::now();
      manifest_entry me;
      me.hash = entry.hash;
      me.timestamp = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::seconds>(
              now.time_since_epoch()).count());
      me.size = entry.data.size();

      // Check if already in manifest
      bool found = false;
      for (auto &e : m.entries) {
        if (e.hash == me.hash) { found = true; break; }
      }
      if (!found) {
        m.entries.push_back(me);
        m.new_additions.push_back(me.hash);
        m.dirty = true;
      }

      // Write manifest every 20 PUTs (shared counter across all workers)
      int count = get_write_queue().put_counter.fetch_add(1) + 1;
      if (count % 20 == 0) {
        need_manifest_write = true;
      }
    }
    // Write manifest outside the lock to avoid deadlock.
    // Only one worker will do this (the one that hits the % 20 boundary).
    if (need_manifest_write) {
      write_manifest_to_s3();
    }
  }
}

// Periodic manifest flush (every 5s if dirty).
void manifest_flush_loop() {
  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    if (!lupine_dedup_s3_configured()) break;
    if (!g_s3_available.load()) continue;
    s3_manifest &m = get_manifest();
    bool need_flush = false;
    {
      std::lock_guard<std::mutex> lock(m.mutex);
      need_flush = m.dirty;
    }
    if (need_flush) {
      write_manifest_to_s3();
    }
  }
}

// Background threads (started per child process).
std::vector<std::thread> g_write_threads;
std::thread g_availability_thread;
std::thread g_flush_thread;

} // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

int lupine_dedup_s3_available() {
  return g_s3_available.load();
}

void lupine_dedup_s3_init() {
  if (!lupine_dedup_s3_configured()) return;

  // Load manifest (or build from bucket scan)
  load_manifest_from_s3();

  // Test availability
  if (test_s3_connection()) {
    g_s3_available.store(1);
    LUPINE_LOG_DEBUG("S3 L2 cache available");
  } else {
    g_s3_available.store(0);
    LUPINE_LOG_ERROR("S3 L2 cache unavailable at init");
  }

  // Start background threads — multiple write workers for parallel S3 uploads
  for (int i = 0; i < S3_WRITE_WORKER_COUNT; i++) {
    g_write_threads.emplace_back(write_through_worker);
  }
  g_availability_thread = std::thread(availability_test_loop);
  g_flush_thread = std::thread(manifest_flush_loop);
}

void lupine_dedup_s3_destroy() {
  if (!lupine_dedup_s3_configured()) return;

  // Signal shutdown
  {
    std::lock_guard<std::mutex> lock(get_write_queue().mutex);
    get_write_queue().shutdown = true;
  }
  get_write_queue().cv.notify_all();

  // Best-effort manifest flush
  if (g_s3_available.load()) {
    write_manifest_to_s3();
  }

  // Detach threads (they'll exit when the child process exits)
  for (auto &t : g_write_threads) {
    if (t.joinable()) t.detach();
  }
  if (g_availability_thread.joinable()) g_availability_thread.detach();
  if (g_flush_thread.joinable()) g_flush_thread.detach();
}

bool lupine_dedup_s3_lookup(const lupine_dedup_hash128 &hash,
                             size_t expected_size, void *dst) {
  if (!lupine_dedup_s3_available()) return false;

  const s3_config &cfg = get_s3_config();
  std::string key = hash_to_s3_key(hash);
  std::vector<unsigned char> data;
  if (!s3_get_object(cfg, key, data)) return false;

  // Check if the S3 data is compressed (size != expected) or raw (size == expected).
  if (data.size() == expected_size) {
    std::memcpy(dst, data.data(), expected_size);
    return true;
  }

  // LZ4-decompress
  int decompressed = LZ4_decompress_safe(
      reinterpret_cast<const char *>(data.data()),
      static_cast<char *>(dst),
      static_cast<int>(data.size()),
      static_cast<int>(expected_size));
  return (decompressed > 0 &&
          static_cast<size_t>(decompressed) == expected_size);
}

void lupine_dedup_s3_insert_async(const lupine_dedup_hash128 &hash,
                                   const void *data, size_t size,
                                   const void *compressed_data,
                                   size_t compressed_size) {
  if (!lupine_dedup_s3_available()) return;

  write_queue_entry entry;
  entry.hash = hash;

  // Prefer compressed bytes (saves S3 storage + bandwidth).
  if (compressed_data != nullptr && compressed_size > 0) {
    entry.data.assign(static_cast<const unsigned char *>(compressed_data),
                      static_cast<const unsigned char *>(compressed_data) + compressed_size);
  } else {
    entry.data.assign(static_cast<const unsigned char *>(data),
                      static_cast<const unsigned char *>(data) + size);
  }

  std::lock_guard<std::mutex> lock(get_write_queue().mutex);
  if (get_write_queue().entries.size() >= write_queue::MAX_ENTRIES) {
    // Queue full — drop this entry. The chunk is still in L1; S3
    // replication is best-effort.
    LUPINE_LOG_ERROR("S3 write queue full, dropping chunk");
    return;
  }
  get_write_queue().entries.push_back(std::move(entry));
  get_write_queue().cv.notify_all();  // wake all workers for parallel processing
}

void lupine_dedup_s3_scan_hashes(std::vector<lupine_dedup_hash128> *out) {
  if (!lupine_dedup_s3_configured()) return;
  s3_manifest &m = get_manifest();
  std::lock_guard<std::mutex> lock(m.mutex);
  out->reserve(out->size() + m.entries.size());
  for (auto &e : m.entries) {
    out->push_back(e.hash);
  }
}

void lupine_dedup_s3_drain_additions(
    std::vector<lupine_dedup_hash128> *out) {
  if (!lupine_dedup_s3_available()) return;
  s3_manifest &m = get_manifest();
  std::lock_guard<std::mutex> lock(m.mutex);
  if (m.new_additions.empty()) return;
  out->insert(out->end(), m.new_additions.begin(), m.new_additions.end());
  m.new_additions.clear();
}

int lupine_dedup_s3_cleanup(size_t byte_cap) {
  if (!lupine_dedup_s3_available()) return -1;

  s3_manifest &m = get_manifest();
  std::vector<manifest_entry> entries_copy;
  {
    std::lock_guard<std::mutex> lock(m.mutex);
    entries_copy = m.entries;
  }

  // Compute total size
  uint64_t total = 0;
  for (auto &e : entries_copy) total += e.size;

  if (total <= byte_cap) {
    LUPINE_LOG_DEBUG("S3 cleanup: total " + std::to_string(total) +
                     " <= cap " + std::to_string(byte_cap) + ", nothing to evict");
    return 0;
  }

  // Sort by timestamp ascending (oldest first)
  std::sort(entries_copy.begin(), entries_copy.end(),
            [](const manifest_entry &a, const manifest_entry &b) {
              return a.timestamp < b.timestamp;
            });

  // Evict oldest until we're under 90% of cap (10% headroom)
  uint64_t target = static_cast<uint64_t>(byte_cap * 9 / 10);
  std::vector<std::string> keys_to_delete;
  std::vector<lupine_dedup_hash128> evicted_hashes;
  for (auto &e : entries_copy) {
    if (total <= target) break;
    keys_to_delete.push_back(hash_to_s3_key(e.hash));
    evicted_hashes.push_back(e.hash);
    total -= e.size;
    if (keys_to_delete.size() >= 1000) {
      // Batch delete
      s3_delete_objects(get_s3_config(), keys_to_delete);
      keys_to_delete.clear();
    }
  }
  if (!keys_to_delete.empty()) {
    s3_delete_objects(get_s3_config(), keys_to_delete);
  }

  // Update manifest: remove evicted entries
  {
    std::lock_guard<std::mutex> lock(m.mutex);
    std::vector<manifest_entry> new_entries;
    new_entries.reserve(m.entries.size() - evicted_hashes.size());
    for (auto &e : m.entries) {
      bool evicted = false;
      for (auto &eh : evicted_hashes) {
        if (e.hash == eh) { evicted = true; break; }
      }
      if (!evicted) new_entries.push_back(e);
    }
    m.entries = std::move(new_entries);
    m.dirty = true;
  }

  // Write updated manifest
  write_manifest_to_s3();

  LUPINE_LOG_DEBUG("S3 cleanup: evicted " + std::to_string(evicted_hashes.size()) +
                   " chunks, new total " + std::to_string(total));
  return 0;
}
