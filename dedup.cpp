// Content-addressed payload dedup cache. See dedup.h and
// docs/dedup-architecture.md for the full design.
//
// This file implements both halves of the dedup cache:
//   - lupine_dedup_server_cache: disk-backed cache of (hash -> chunk file).
//     Chunks are stored as files in a sharded directory; the filesystem IS
//     the index. Shared across all connections and all forked server
//     children (every disk_cache_impl instance points at the same
//     LUPINE_DEDUP_CACHE_DIR). Persists across server restarts.
//   - lupine_dedup_client_cache: per-client-process mirror set of hashes,
//     so the writer can decide hit-vs-miss without a round-trip.
//
// The hash function is XXH3_128bits from vendored xxHash (third_party/xxhash,
// BSD-2-Clause). It runs at ~10-15 GB/s on x86 with AVX2 and is formally
// analyzed (SMHasher-certified).

#include "dedup.h"
#include "dedup_s3.h"
#include "rpc.h"
#include "lupine_log.h"
#include "codegen/gen_api.h"  // LUPINE_RPC_lupineDedupHashList

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <lz4.h>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <array>
#include <deque>
#include <xxhash.h>

void lupine_dedup_hash(const void *data, size_t len,
                       lupine_dedup_hash128 *out) {
  // XXH3_128bits: formally analyzed, SMHasher-certified 128-bit hash.
  // Runs at ~10-15 GB/s on x86 with AVX2. Vendored from third_party/xxhash
  // (BSD-2-Clause, same license family as the vendored LZ4).
  XXH128_hash_t h = XXH3_128bits(data, len);
  std::memcpy(out->bytes, &h, sizeof(h));
}

// ---------------------------------------------------------------------------
// Global enable / config.
// ---------------------------------------------------------------------------

int lupine_dedup_enabled_globally() {
  static int cached = [] {
    const char *v = std::getenv("LUPINE_DEDUP");
    // Default OFF. Set LUPINE_DEDUP=1 to enable.
    return (v != nullptr && std::strcmp(v, "1") == 0) ? 1 : 0;
  }();
  return cached;
}

size_t lupine_dedup_server_byte_cap_default() {
  static size_t cached = [] {
    const char *v = std::getenv("LUPINE_DEDUP_CACHE_BYTES");
    if (v == nullptr || *v == '\0') {
      // Default for disk cache: 4 GiB (holds ~1024 chunks of 4 MiB).
      // Users with large models should set LUPINE_DEDUP_CACHE_BYTES higher.
      return static_cast<size_t>(4) * 1024 * 1024 * 1024;
    }
    char *end = nullptr;
    unsigned long long parsed = std::strtoull(v, &end, 10);
    if (end == v || parsed == 0) {
      return static_cast<size_t>(4) * 1024 * 1024 * 1024;
    }
    return static_cast<size_t>(parsed);
  }();
  return cached;
}

int lupine_dedup_compress_cache_enabled() {
  static int cached = [] {
    const char *v = std::getenv("LUPINE_DEDUP_COMPRESS_CACHE");
    // Default ON. Set LUPINE_DEDUP_COMPRESS_CACHE=0 to disable.
    return (v == nullptr || std::strcmp(v, "0") != 0) ? 1 : 0;
  }();
  return cached;
}

int lupine_dedup_enabled(conn_t *conn) {
  if (conn == nullptr || !lupine_dedup_enabled_globally()) {
    return 0;
  }
  return (conn->dedup_server_cache != nullptr ||
          conn->dedup_client_cache != nullptr)
             ? 1
             : 0;
}

// ---------------------------------------------------------------------------
// Server-side disk cache.
//
// Chunks are stored as individual files in a sharded directory structure.
// The filesystem IS the index (open() is the lookup, unlink() is the delete).
// The OS page cache is the implicit RAM hot tier — no explicit L1 needed.
//
// Layout:
//   <cache_dir>/chunks/ab/cdef0123456789...   (4 MiB file per chunk)
//   <cache_dir>/size                          (uint64: current total bytes)
//   <cache_dir>/invalidations.log             (append-only, 16 bytes per eviction)
//
// The cache is shared across all connections and all forked server children
// (every disk_cache_impl instance points at the same cache_dir, read from
// LUPINE_DEDUP_CACHE_DIR). Cross-process eviction notifications flow through
// invalidations.log, which each child drains on every response.
//
// The cache persists across server restarts. On restart, existing chunk
// files are reused (O_CREAT|O_EXCL skips re-writing existing files). The
// size file maintains the running total via incremental updates; it is
// not read back on restart. The invalidation log is NOT truncated (each
// child seeks to end on open, only reading new entries).
// ---------------------------------------------------------------------------

namespace {

// Returns the cache directory from LUPINE_DEDUP_CACHE_DIR or default
// ~/.cache/lupine-dedup. Read fresh each call so tests can change the env
// var between test cases.
std::string lupine_dedup_cache_dir() {
  const char *env = std::getenv("LUPINE_DEDUP_CACHE_DIR");
  if (env && *env) return std::string(env);
  const char *home = std::getenv("HOME");
  if (!home || !*home) home = "/tmp";
  return std::string(home) + "/.cache/lupine-dedup";
}

// Converts a 128-bit hash to a sharded file path:
//   <cache_dir>/chunks/ab/cdef0123456789abcdef0123456789abcdef
// The first 2 hex chars form a subdirectory (256 shards, ~40 files each
// for 10K chunks) to avoid huge single directories.
std::string hash_to_chunk_path(const std::string &cache_dir,
                               const lupine_dedup_hash128 &hash) {
  char hex[33];
  for (int i = 0; i < 16; ++i) {
    std::snprintf(hex + i * 2, 3, "%02x", hash.bytes[i]);
  }
  return cache_dir + "/chunks/" + std::string(hex, 2) + "/" +
         std::string(hex + 2);
}

// Extracts the hash from a chunk file path (reverse of hash_to_chunk_path).
// Returns true on success, false if the path doesn't match the expected format.
bool chunk_path_to_hash(const std::string &path, lupine_dedup_hash128 *out) {
  // Path format: .../chunks/ab/cdef0123456789...
  // Find the last "/" and extract 30 hex chars after it.
  size_t pos = path.rfind('/');
  if (pos == std::string::npos || pos < 2) return false;
  // The 2-char shard is at path[pos-2..pos), the 30-char rest is after pos.
  if (pos + 30 > path.size()) return false;
  char hex[33];
  hex[0] = path[pos - 2];
  hex[1] = path[pos - 1];
  std::memcpy(hex + 2, path.c_str() + pos + 1, 30);
  hex[32] = '\0';
  for (int i = 0; i < 16; ++i) {
    unsigned int byte;
    if (std::sscanf(hex + i * 2, "%02x", &byte) != 1) return false;
    out->bytes[i] = static_cast<uint8_t>(byte);
  }
  return true;
}

// Creates a directory and all parents (like mkdir -p). Returns 0 on success
// or if the dir already exists, -1 on failure.
int mkdir_p(const char *path) {
  // Start from the beginning, creating each component.
  std::string s(path);
  for (size_t i = 1; i < s.size(); ++i) {
    if (s[i] == '/') {
      s[i] = '\0';
      mkdir(s.c_str(), 0700);  // ignore error (EEXIST is fine)
      s[i] = '/';
    }
  }
  if (mkdir(s.c_str(), 0700) != 0 && errno != EEXIST) return -1;
  return 0;
}

// Updates the size file using fcntl lock for cross-process safety.
// `delta` is added to the current value. Returns the new value.
size_t update_size_file(const std::string &cache_dir, int64_t delta) {
  std::string path = cache_dir + "/size";
  int fd = open(path.c_str(), O_RDWR | O_CREAT, 0600);
  if (fd < 0) return 0;
  // Lock the entire file (1 byte at offset 0 is enough, but lock the whole
  // thing to be safe).
  struct flock fl;
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = sizeof(uint64_t);
  if (fcntl(fd, F_SETLKW, &fl) < 0) {
    close(fd);
    return 0;
  }
  uint64_t val = 0;
  if (pread(fd, &val, sizeof(val), 0) != sizeof(val)) val = 0;
  if (delta >= 0) {
    val += static_cast<uint64_t>(delta);
  } else {
    uint64_t d = static_cast<uint64_t>(-delta);
    val = (val > d) ? val - d : 0;
  }
  pwrite(fd, &val, sizeof(val), 0);
  fl.l_type = F_UNLCK;
  fcntl(fd, F_SETLK, &fl);
  close(fd);
  return static_cast<size_t>(val);
}

// Appends a hash to the shared invalidation log. Any process can append
// (O_APPEND makes small writes atomic on most filesystems). Other processes
// drain the log on each response.
void append_invalidation_log(const std::string &cache_dir,
                             const lupine_dedup_hash128 &hash) {
  std::string path = cache_dir + "/invalidations.log";
  int fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0600);
  if (fd < 0) return;
  write(fd, hash.bytes, sizeof(hash.bytes));
  close(fd);
}

// Appends a hash to the shared additions log. Same pattern as
// append_invalidation_log, but for insertions (not evictions). Other
// processes drain the log on each response and forward additions to
// their clients via the bit-31 response prefix.
void append_addition_log(const std::string &cache_dir,
                          const lupine_dedup_hash128 &hash) {
  std::string path = cache_dir + "/additions.log";
  int fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0600);
  if (fd < 0) return;
  write(fd, hash.bytes, sizeof(hash.bytes));
  close(fd);
}

// Scans the chunks directory and collects all chunk files with their mtimes
// and sizes. Used for eviction.
struct chunk_file_info {
  std::string path;
  time_t mtime;
  size_t size;
};

void scan_chunks_dir(const std::string &chunks_dir,
                     std::vector<chunk_file_info> *out) {
  DIR *dir = opendir(chunks_dir.c_str());
  if (!dir) return;
  struct dirent *ent;
  while ((ent = readdir(dir)) != nullptr) {
    if (ent->d_name[0] == '.') continue;
    std::string shard_path = chunks_dir + "/" + ent->d_name;
    DIR *shard = opendir(shard_path.c_str());
    if (!shard) continue;
    struct dirent *sent;
    while ((sent = readdir(shard)) != nullptr) {
      if (sent->d_name[0] == '.') continue;
      std::string file_path = shard_path + "/" + sent->d_name;
      struct stat st;
      if (stat(file_path.c_str(), &st) == 0 && S_ISREG(st.st_mode)) {
        out->push_back({file_path, st.st_mtime,
                        static_cast<size_t>(st.st_size)});
      }
    }
    closedir(shard);
  }
  closedir(dir);
}

struct disk_cache_impl {
  std::string cache_dir;
  size_t byte_cap = 0;
  std::mutex mutex;  // protects pending_*, send_*, additions_*
  std::vector<lupine_dedup_hash128> pending_invalidations;
  std::vector<lupine_dedup_hash128> pending_additions;
  std::vector<lupine_dedup_hash128> send_buffer;       // reused for inv or add
  std::vector<lupine_dedup_hash128> additions_send_buffer;
  uint32_t send_count = 0;
  uint32_t additions_send_count = 0;
  uint32_t inv_count_word = 0;  // stored here so rpc_write pointer is valid
  int invalidation_log_fd = -1;     // for reading new evictions by other procs
  off_t invalidation_log_offset = 0;  // last read position
  int additions_log_fd = -1;        // for reading new inserts by other procs
  off_t additions_log_offset = 0;   // last read position
};

} // namespace

lupine_dedup_server_cache *lupine_dedup_server_create(size_t byte_cap) {
  auto *impl = new disk_cache_impl;
  impl->cache_dir = lupine_dedup_cache_dir();
  impl->byte_cap = byte_cap;

  // Create directory structure
  mkdir_p(impl->cache_dir.c_str());
  mkdir_p((impl->cache_dir + "/chunks").c_str());

  // Open invalidation log for reading. Seek to end so we only read new
  // entries (evictions by other processes that happen after our start).
  std::string inv_path = impl->cache_dir + "/invalidations.log";
  impl->invalidation_log_fd = open(inv_path.c_str(), O_RDONLY | O_CREAT, 0600);
  if (impl->invalidation_log_fd >= 0) {
    impl->invalidation_log_offset = lseek(impl->invalidation_log_fd, 0, SEEK_END);
  }

  // Open additions log for reading. Same pattern: seek to end so we only
  // read new inserts by other processes after our start.
  std::string add_path = impl->cache_dir + "/additions.log";
  impl->additions_log_fd = open(add_path.c_str(), O_RDONLY | O_CREAT, 0600);
  if (impl->additions_log_fd >= 0) {
    impl->additions_log_offset = lseek(impl->additions_log_fd, 0, SEEK_END);
  }

  return reinterpret_cast<lupine_dedup_server_cache *>(impl);
}

void lupine_dedup_server_destroy(lupine_dedup_server_cache *c) {
  if (c == nullptr) return;
  auto *impl = reinterpret_cast<disk_cache_impl *>(c);
  // NOTE: We deliberately do NOT delete the cache directory. The disk cache
  // persists across server restarts. Only close file descriptors.
  if (impl->invalidation_log_fd >= 0) {
    close(impl->invalidation_log_fd);
  }
  if (impl->additions_log_fd >= 0) {
    close(impl->additions_log_fd);
  }
  delete impl;
}

void lupine_dedup_server_insert(lupine_dedup_server_cache *c,
                                const void *chunk, size_t chunk_size,
                                lupine_dedup_hash128 *out_hash,
                                const void *compressed_data,
                                size_t compressed_size) {
  if (c == nullptr || chunk == nullptr || chunk_size == 0) {
    if (out_hash) std::memset(out_hash->bytes, 0, 16);
    return;
  }
  auto *impl = reinterpret_cast<disk_cache_impl *>(c);

  lupine_dedup_hash128 h;
  lupine_dedup_hash(chunk, chunk_size, &h);
  if (out_hash) *out_hash = h;

  // If the chunk is larger than the entire cache cap, we can't cache it.
  // Tell the client to remove this hash from its mirror (Option A).
  if (chunk_size > impl->byte_cap) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending_invalidations.push_back(h);
    return;
  }

  std::string path = hash_to_chunk_path(impl->cache_dir, h);

  // Create the shard directory if it doesn't exist (chunks/ab/). The parent
  // (chunks/) is created in lupine_dedup_server_create, but the per-hash
  // shard dir is created lazily here. mkdir is idempotent (EEXIST is fine).
  size_t last_slash = path.rfind('/');
  if (last_slash != std::string::npos) {
    std::string shard_dir = path.substr(0, last_slash);
    mkdir(shard_dir.c_str(), 0700);  // ignore error (EEXIST is fine)
  }

  // Try to create the file with O_CREAT|O_EXCL. If it already exists (chunk
  // was cached in a previous run or by another fork), skip the write.
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0) {
    if (errno == EEXIST) {
      // File already exists — chunk is already cached. No write needed,
      // no size change, no eviction needed. Just return.
      return;
    }
    // Can't create the file (disk full, permission error, etc.). Treat as
    // insert failure — tell the client to invalidate (Option A).
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending_invalidations.push_back(h);
    return;
  }

  // Determine what to write to disk: compressed bytes (if compression is
  // enabled and we have them), or raw bytes.
  //
  // When compression is enabled (default), we prefer the compressed form
  // the caller already has (from the LZ4-framed wire transfer). If the
  // caller doesn't have compressed bytes (token==0 raw block), we compress
  // here. This halves disk usage for typical compressible data.
  //
  // When compression is disabled, we store raw bytes for faster lookups
  // (no decompression needed on HIT).
  const void *write_data = chunk;
  size_t write_size = chunk_size;
  std::vector<unsigned char> compressed_buf;  // holds compressed bytes if we compress here

  if (lupine_dedup_compress_cache_enabled()) {
    if (compressed_data != nullptr && compressed_size > 0) {
      // Caller provided the compressed form (LZ4-framed MISS path) — use it.
      write_data = compressed_data;
      write_size = compressed_size;
    } else {
      // Raw block (token==0) — compress it ourselves. This happens when
      // LZ4 decided the data was incompressible (stored raw on the wire).
      // We still compress for storage because even "incompressible" 4 MiB
      // blocks often have some redundancy that LZ4 can exploit.
      int bound = LZ4_compressBound(static_cast<int>(chunk_size));
      compressed_buf.resize(static_cast<size_t>(bound));
      int compressed_len = LZ4_compress_default(
          static_cast<const char *>(chunk),
          reinterpret_cast<char *>(compressed_buf.data()),
          static_cast<int>(chunk_size), bound);
      if (compressed_len > 0 &&
          static_cast<size_t>(compressed_len) < chunk_size) {
        write_data = compressed_buf.data();
        write_size = static_cast<size_t>(compressed_len);
      }
      // If compression didn't help (compressed_len >= chunk_size), fall
      // through and store raw. This is rare for real model data.
    }
  }

  // Write the data to the file.
  ssize_t written = write(fd, write_data, write_size);
  close(fd);
  if (written < 0 || static_cast<size_t>(written) != write_size) {
    // Write failed — remove the partial file and invalidate.
    unlink(path.c_str());
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending_invalidations.push_back(h);
    return;
  }

  // Update the shared size file with the actual bytes written (compressed
  // size if compression is on, raw size if off).
  size_t new_total = update_size_file(impl->cache_dir,
                                      static_cast<int64_t>(write_size));

  // If over cap, evict oldest chunks until we're back under.
  if (new_total > impl->byte_cap) {
    std::vector<chunk_file_info> files;
    scan_chunks_dir(impl->cache_dir + "/chunks", &files);
    // Sort by mtime ascending (oldest first = FIFO eviction).
    std::sort(files.begin(), files.end(),
              [](const chunk_file_info &a, const chunk_file_info &b) {
                return a.mtime < b.mtime;
              });
    // Evict until we're under cap. Leave 10% headroom to avoid evicting
    // on every single insert.
    //
    // When S3 L2 is enabled, skip chunks that haven't been uploaded to S3
    // yet — evicting them would cause a hard failure if a client sends a
    // DEDUP_REF for that chunk (L1 miss → S3 miss → hard failure). The
    // chunk stays in L1 until the S3 upload completes, at which point a
    // future eviction pass can remove it safely.
    //
    // When S3 is not enabled, lupine_dedup_s3_has_hash always returns 1,
    // so all chunks are eligible for eviction (current behavior — evict
    // and log to invalidations.log).
    size_t target = impl->byte_cap * 9 / 10;
    for (const auto &f : files) {
      if (new_total <= target) break;
      lupine_dedup_hash128 evicted_hash;
      if (!chunk_path_to_hash(f.path, &evicted_hash)) continue;
      // Skip if S3 upload hasn't completed yet (S3 is the safety net)
      if (!lupine_dedup_s3_has_hash(evicted_hash)) continue;
      if (unlink(f.path.c_str()) == 0) {
        new_total = update_size_file(impl->cache_dir,
                                     -static_cast<int64_t>(f.size));
        append_invalidation_log(impl->cache_dir, evicted_hash);
      }
    }
  }

  // Notify other forked children (and their clients) that this chunk is now
  // cached. The additions.log is drained by each child on its next response
  // flush, and the hashes are forwarded to clients via the bit-31 prefix.
  append_addition_log(impl->cache_dir, h);

  // Async write-through to S3 L2 (if configured). Non-blocking — the data
  // is copied into the write queue. If the queue is full, the entry is
  // dropped (best-effort; the chunk is still in L1).
  if (lupine_dedup_s3_available()) {
    lupine_dedup_s3_insert_async(h, chunk, chunk_size,
                                  write_data, write_size);
  }
}

bool lupine_dedup_server_lookup(lupine_dedup_server_cache *c,
                                const lupine_dedup_hash128 &hash,
                                size_t expected_chunk_size, void *dst) {
  if (c == nullptr) return false;
  auto *impl = reinterpret_cast<disk_cache_impl *>(c);

  std::string path = hash_to_chunk_path(impl->cache_dir, hash);
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    // Local disk miss. Try S3 L2 if configured. On S3 hit, the bytes are
    // written to dst and the caller will promote them to L1 via
    // lupine_dedup_server_insert (which will get EEXIST on the O_CREAT|O_EXCL
    // if another thread raced us, or will write a fresh L1 copy).
    if (lupine_dedup_s3_available() &&
        lupine_dedup_s3_lookup(hash, expected_chunk_size, dst)) {
      LUPINE_TRACE_LOG("LUPINE dedup S2 HIT chunk_size=" << expected_chunk_size);
      return true;
    }
    // File doesn't exist on disk or S3 — miss. Could be: never cached,
    // evicted by this process, or evicted by another fork. The caller
    // handles this as a miss (Option A invalidation if the client
    // expected a HIT).
    return false;
  }

  // Get the file size to know whether it's compressed or raw.
  struct stat st;
  if (fstat(fd, &st) < 0) {
    close(fd);
    return false;
  }
  size_t file_size = static_cast<size_t>(st.st_size);

  bool compressed = lupine_dedup_compress_cache_enabled() &&
                    file_size != expected_chunk_size;

  if (!compressed) {
    // Uncompressed: read directly into dst. This covers both the
    // compression-disabled case and the rare case where compression
    // didn't help (file_size == expected_chunk_size).
    ssize_t n = read(fd, dst, expected_chunk_size);
    close(fd);
    return (n > 0 && static_cast<size_t>(n) == expected_chunk_size);
  }

  // Compressed: read the compressed bytes, then LZ4-decompress into dst.
  std::vector<unsigned char> compressed_buf(file_size);
  ssize_t n = read(fd, compressed_buf.data(), file_size);
  close(fd);
  if (n <= 0 || static_cast<size_t>(n) != file_size) {
    return false;
  }
  int decompressed = LZ4_decompress_safe(
      reinterpret_cast<const char *>(compressed_buf.data()),
      static_cast<char *>(dst), static_cast<int>(file_size),
      static_cast<int>(expected_chunk_size));
  return (decompressed > 0 &&
          static_cast<size_t>(decompressed) == expected_chunk_size);
}

void lupine_dedup_server_scan_hashes(lupine_dedup_server_cache *c,
                                     std::vector<lupine_dedup_hash128> *out) {
  if (c == nullptr || out == nullptr) return;
  auto *impl = reinterpret_cast<disk_cache_impl *>(c);
  // Reuse the existing scan_chunks_dir helper (defined above) to collect
  // all chunk files, then extract the hash from each file path.
  std::vector<chunk_file_info> files;
  scan_chunks_dir(impl->cache_dir + "/chunks", &files);
  out->clear();
  out->reserve(files.size());
  for (const auto &f : files) {
    lupine_dedup_hash128 h;
    if (chunk_path_to_hash(f.path, &h)) {
      out->push_back(h);
    }
  }
}

int lupine_dedup_server_flush_invalidations(lupine_dedup_server_cache *c,
                                            conn_t *conn) {
  if (c == nullptr) return 0;
  auto *impl = reinterpret_cast<disk_cache_impl *>(c);

  // Drain the shared invalidation log: read any new entries appended by
  // other forked processes since our last drain. These are hashes that
  // were evicted by other children — our client's mirror is stale for them.
  if (impl->invalidation_log_fd >= 0) {
    for (;;) {
      lupine_dedup_hash128 h;
      ssize_t n = pread(impl->invalidation_log_fd, h.bytes,
                        sizeof(h.bytes), impl->invalidation_log_offset);
      if (n != sizeof(h.bytes)) break;  // no more entries
      impl->invalidation_log_offset += n;
      std::lock_guard<std::mutex> lock(impl->mutex);
      impl->pending_invalidations.push_back(h);
    }
  }

  // Drain the shared additions log: read any new entries appended by
  // other forked processes since our last drain. These are hashes that
  // were newly cached by other children — our client's mirror should
  // learn about them so future uploads get DEDUP_REF HITs.
  if (impl->additions_log_fd >= 0) {
    for (;;) {
      lupine_dedup_hash128 h;
      ssize_t n = pread(impl->additions_log_fd, h.bytes,
                        sizeof(h.bytes), impl->additions_log_offset);
      if (n != sizeof(h.bytes)) break;  // no more entries
      impl->additions_log_offset += n;
      std::lock_guard<std::mutex> lock(impl->mutex);
      impl->pending_additions.push_back(h);
    }
  }

  // Also drain S3 additions (new hashes from S3 manifest refreshes).
  if (lupine_dedup_s3_available()) {
    lupine_dedup_s3_drain_additions(&impl->pending_additions);
  }

  // Swap pending invalidations into send_buffer. The send_buffer and
  // send_count live in the cache struct (which lives as long as the
  // connection), so the pointers pushed onto the write queue by rpc_write
  // remain valid until rpc_write_end sends them.
  {
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->send_buffer.swap(impl->pending_invalidations);
    impl->send_count = static_cast<uint32_t>(impl->send_buffer.size());

    // Cap additions at 4096 per response to bound the prefix size.
    // Remaining additions stay in pending_additions for next response.
    size_t add_to_send = std::min(impl->pending_additions.size(),
                                   static_cast<size_t>(4096));
    impl->additions_send_buffer.assign(
        impl->pending_additions.begin(),
        impl->pending_additions.begin() + add_to_send);
    impl->pending_additions.erase(
        impl->pending_additions.begin(),
        impl->pending_additions.begin() + add_to_send);
    impl->additions_send_count =
        static_cast<uint32_t>(impl->additions_send_buffer.size());
  }

  // Build the inv_count word. If there are additions, set bit 31 to
  // signal "additions follow after the invalidations."
  // Store in impl so the pointer handed to rpc_write remains valid
  // until rpc_write_end sends it (rpc_write stores pointers, not copies).
  impl->inv_count_word = impl->send_count;
  bool has_additions = impl->additions_send_count > 0;
  if (has_additions) {
    impl->inv_count_word |= 0x80000000u;
  }

  // Write [inv_count]
  if (rpc_write(conn, &impl->inv_count_word, sizeof(impl->inv_count_word)) < 0)
    return -1;

  // Write [inv_count × 16-byte hashes]
  if (impl->send_count > 0) {
    if (rpc_write(conn, impl->send_buffer.data(),
                  impl->send_count * sizeof(lupine_dedup_hash128)) < 0)
      return -1;
  }

  // Write [add_count][add × 16-byte hashes] (only if bit 31 was set)
  if (has_additions) {
    if (rpc_write(conn, &impl->additions_send_count,
                  sizeof(impl->additions_send_count)) < 0)
      return -1;
    if (impl->additions_send_count > 0) {
      if (rpc_write(conn, impl->additions_send_buffer.data(),
                    impl->additions_send_count *
                        sizeof(lupine_dedup_hash128)) < 0)
        return -1;
    }
  }

  return 0;
}

// Called after a successful local-disk insert to append the hash to the
// shared additions.log so other forked children (and their clients) learn
// about the new cache entry.
void lupine_dedup_server_append_addition(const lupine_dedup_hash128 &hash) {
  std::string cache_dir = lupine_dedup_cache_dir();
  append_addition_log(cache_dir, hash);
}

// ---------------------------------------------------------------------------
// Client-side mirror.
// ---------------------------------------------------------------------------

namespace {

struct client_cache_impl {
  std::unordered_set<lupine_dedup_hash128> present;
  std::mutex mutex;
};

} // namespace

lupine_dedup_client_cache *lupine_dedup_client_create() {
  auto *c = new client_cache_impl;
  return reinterpret_cast<lupine_dedup_client_cache *>(c);
}

void lupine_dedup_client_destroy(lupine_dedup_client_cache *c) {
  if (c == nullptr) return;
  delete reinterpret_cast<client_cache_impl *>(c);
}

bool lupine_dedup_client_has(lupine_dedup_client_cache *c,
                             const lupine_dedup_hash128 &hash) {
  if (c == nullptr) return false;
  auto *impl = reinterpret_cast<client_cache_impl *>(c);
  std::lock_guard<std::mutex> lock(impl->mutex);
  return impl->present.find(hash) != impl->present.end();
}

void lupine_dedup_client_add(lupine_dedup_client_cache *c,
                             const lupine_dedup_hash128 &hash) {
  if (c == nullptr) return;
  auto *impl = reinterpret_cast<client_cache_impl *>(c);
  std::lock_guard<std::mutex> lock(impl->mutex);
  impl->present.insert(hash);
}

void lupine_dedup_client_invalidate(lupine_dedup_client_cache *c,
                                    const lupine_dedup_hash128 *hashes,
                                    size_t count) {
  if (c == nullptr || hashes == nullptr || count == 0) return;
  auto *impl = reinterpret_cast<client_cache_impl *>(c);
  std::lock_guard<std::mutex> lock(impl->mutex);
  for (size_t i = 0; i < count; ++i) {
    impl->present.erase(hashes[i]);
  }
}

void lupine_dedup_client_clear(lupine_dedup_client_cache *c) {
  if (c == nullptr) return;
  auto *impl = reinterpret_cast<client_cache_impl *>(c);
  std::lock_guard<std::mutex> lock(impl->mutex);
  impl->present.clear();
}

// ---------------------------------------------------------------------------
// Payload hooks -- the full dedup + LZ4 logic, called from compress.cpp.
// ---------------------------------------------------------------------------

namespace {

// Returns true if the token is in the valid LZ4-compressed-size range for a
// chunk of `raw` uncompressed bytes. Anything outside this range (other than
// 0 = raw, or LUPINE_DEDUP_REF_TOKEN) is a protocol error.
bool lupine_dedup_token_is_valid_lz4(uint32_t token, size_t raw) {
  return token != 0 && token != LUPINE_DEDUP_REF_TOKEN &&
         token <= static_cast<uint32_t>(
                      LZ4_compressBound(static_cast<int>(raw)));
}

// DEDUP_REF tokens and hashes are emitted via rpc_write, which stores
// POINTERS to the caller's data on the write queue (not copies). Those
// pointers must remain valid until rpc_write_end sends them.
//
// We use a thread_local std::deque of fixed-size 20-byte blocks. Unlike
// std::vector, std::deque does NOT relocate existing elements when a new
// element is pushed to the back -- so pointers handed to earlier rpc_write()
// calls stay valid until rpc_write_end() flushes the queue. A std::vector
// here would be wrong: it reallocates as it grows, invalidating earlier
// iovecs and corrupting the wire stream on payloads with multiple HITs.
constexpr size_t LUPINE_DEDUP_HIT_BLOCK_BYTES =
    sizeof(uint32_t) + sizeof(lupine_dedup_hash128);  // 4 + 16 = 20
std::deque<std::array<unsigned char, LUPINE_DEDUP_HIT_BLOCK_BYTES>> &
dedup_write_scratch() {
  static thread_local
      std::deque<std::array<unsigned char, LUPINE_DEDUP_HIT_BLOCK_BYTES>> scratch;
  return scratch;
}

} // namespace

int lupine_dedup_write_payload(conn_t *conn, const void *data, size_t size) {
  auto *mirror =
      static_cast<lupine_dedup_client_cache *>(conn->dedup_client_cache);
  // Clear the scratch buffer for this call. Each DEDUP_REF token + hash
  // (4 + 16 = 20 bytes) is appended here, and pointers into this buffer
  // are pushed onto the write queue. The buffer survives until the next
  // rpc_write_payload call on this thread, which is always after
  // rpc_write_end has sent the data.
  dedup_write_scratch().clear();

  const auto *src = static_cast<const unsigned char *>(data);
  size_t remaining = size;
  while (remaining > 0) {
    size_t chunk = std::min(static_cast<size_t>(LUPINE_COMPRESS_BLOCK_BYTES),
                            remaining);
    lupine_dedup_hash128 hash;
    lupine_dedup_hash(src, chunk, &hash);

    if (mirror != nullptr && lupine_dedup_client_has(mirror, hash)) {
      // Cache hit: emit DEDUP_REF token + hash. 20 bytes total.
      // We push a new fixed-size node onto the deque and hand rpc_write a
      // pointer into it. std::deque does not relocate existing nodes on
      // emplace_back, so prior pointers stay valid until rpc_write_end
      // flushes them. (A std::vector here would be wrong -- see the
      // rationale on dedup_write_scratch above.)
      auto &scratch = dedup_write_scratch();
      scratch.emplace_back();
      auto &block = scratch.back();
      uint32_t token = LUPINE_DEDUP_REF_TOKEN;
      std::memcpy(block.data(), &token, sizeof(token));
      std::memcpy(block.data() + sizeof(token), hash.bytes,
                  sizeof(hash.bytes));
      if (rpc_write(conn, block.data(), block.size()) < 0) {
        return -1;
      }
      LUPINE_TRACE_LOG("LUPINE dedup write HIT chunk_size=" << chunk
                       << " chunk_offset=" << (size - remaining));
    } else {
      // Cache miss: emit existing LZ4 framing for this single chunk. The
      // HTTP/2 transport handles per-iovec LZ4 framing, so per-chunk
      // rpc_write_framed calls compose correctly. `src` points into the
      // caller's buffer which is valid until rpc_write_end.
      if (rpc_write_framed(conn, src, chunk) < 0) {
        return -1;
      }
      if (mirror != nullptr) {
        lupine_dedup_client_add(mirror, hash);
      }
      LUPINE_TRACE_LOG("LUPINE dedup write MISS chunk_size=" << chunk
                       << " chunk_offset=" << (size - remaining));
    }
    src += chunk;
    remaining -= chunk;
  }
  return 0;
}

int lupine_dedup_read_payload_part(conn_t *conn, void *data, size_t size) {
  auto *cache =
      static_cast<lupine_dedup_server_cache *>(conn->dedup_server_cache);
  auto *dst = static_cast<char *>(data);
  size_t remaining = size;
  char *scratch = nullptr;
  while (remaining > 0) {
    size_t raw = std::min(static_cast<size_t>(LUPINE_COMPRESS_BLOCK_BYTES),
                          remaining);
    uint32_t token = 0;
    if (rpc_read(conn, &token, sizeof(token)) < 0) {
      free(scratch);
      return -1;
    }
    if (token == LUPINE_DEDUP_REF_TOKEN) {
      // Dedup reference: read hash, look up in cache, emit cached bytes.
      lupine_dedup_hash128 hash;
      if (rpc_read(conn, hash.bytes, sizeof(hash.bytes)) < 0) {
        free(scratch);
        return -1;
      }
      if (cache == nullptr ||
          !lupine_dedup_server_lookup(cache, hash, raw, dst)) {
        // Safety net: should never happen given the client mirror, but if
        // it does (e.g., a race after a forced cache reset), fail hard so
        // the caller surfaces the error rather than silently corrupting.
        free(scratch);
        return -1;
      }
      LUPINE_TRACE_LOG("LUPINE dedup read  HIT chunk_size=" << raw
                       << " chunk_offset=" << (size - remaining));
    } else if (token == 0) {
      if (rpc_read(conn, dst, raw) < 0) {
        free(scratch);
        return -1;
      }
      if (cache != nullptr) {
        lupine_dedup_hash128 h;
        // Raw block (token==0): no compressed bytes available, pass nullptr.
        // insert() will compress internally if LUPINE_DEDUP_COMPRESS_CACHE=1.
        lupine_dedup_server_insert(cache, dst, raw, &h, nullptr, 0);
      }
      LUPINE_TRACE_LOG("LUPINE dedup read  MISS(raw) chunk_size=" << raw
                       << " chunk_offset=" << (size - remaining));
    } else {
      if (!lupine_dedup_token_is_valid_lz4(token, raw)) {
        free(scratch);
        return -1;
      }
      if (scratch == nullptr) {
        scratch = static_cast<char *>(malloc(static_cast<size_t>(
            LZ4_compressBound(static_cast<int>(LUPINE_COMPRESS_BLOCK_BYTES)))));
        if (scratch == nullptr) {
          return -1;
        }
      }
      if (rpc_read(conn, scratch, token) < 0 ||
          LZ4_decompress_safe(scratch, dst, static_cast<int>(token),
                              static_cast<int>(raw)) != static_cast<int>(raw)) {
        free(scratch);
        return -1;
      }
      if (cache != nullptr) {
        lupine_dedup_hash128 h;
        // LZ4-compressed block: pass the compressed bytes (in scratch, size=token)
        // to insert() so it can store them directly without recompression.
        lupine_dedup_server_insert(cache, dst, raw, &h, scratch,
                                   static_cast<size_t>(token));
      }
      LUPINE_TRACE_LOG("LUPINE dedup read  MISS(lz4 compressed=" << token
                       << ") chunk_size=" << raw
                       << " chunk_offset=" << (size - remaining));
    }
    dst += raw;
    remaining -= raw;
  }
  free(scratch);
  return static_cast<int>(size);
}

int lupine_dedup_drain_payload(conn_t *conn, size_t size) {
  size_t remaining = size;
  while (remaining > 0) {
    size_t raw = std::min(static_cast<size_t>(LUPINE_COMPRESS_BLOCK_BYTES),
                          remaining);
    uint32_t token = 0;
    if (rpc_read(conn, &token, sizeof(token)) < 0) {
      return -1;
    }
    if (token == LUPINE_DEDUP_REF_TOKEN) {
      // Dedup reference: read and discard the 16-byte hash. The cached
      // bytes stay on the server; nothing to drain from the wire.
      lupine_dedup_hash128 hash;
      if (rpc_read(conn, hash.bytes, sizeof(hash.bytes)) < 0) {
        return -1;
      }
    } else if (token == 0) {
      if (rpc_drain(conn, raw) < 0) {
        return -1;
      }
    } else {
      if (!lupine_dedup_token_is_valid_lz4(token, raw) ||
          rpc_drain(conn, token) < 0) {
        return -1;
      }
    }
    remaining -= raw;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Connection lifecycle hooks -- called from rpc.cpp / h2.cpp.
// ---------------------------------------------------------------------------

void lupine_dedup_conn_init(conn_t *conn, int is_server) {
  if (conn == nullptr || !lupine_dedup_enabled_globally()) {
    return;
  }
  if (is_server) {
    conn->dedup_server_cache = lupine_dedup_server_create(
        lupine_dedup_server_byte_cap_default());
    // Initialize S3 L2 if configured. Starts background threads and loads
    // the manifest. No-op if LUPINE_DEDUP_S3_BUCKET is not set.
    lupine_dedup_s3_init();
  } else {
    conn->dedup_client_cache = lupine_dedup_client_create();
  }
}

// Drain `bytes` bytes from the connection in fixed-size chunks. Used to keep
// the wire frame aligned when we want to abandon a hash-list response without
// consuming the rest of it (e.g. count exceeds the sanity bound, or a batch
// read failed mid-stream). Without this drain, the next rpc_dispatch() read
// would pick up the first bytes of the unread hash data as a request_id and
// corrupt the connection.
static void lupine_dedup_drain_remaining(conn_t *conn, uint64_t bytes) {
  uint8_t scratch[4096];
  while (bytes > 0) {
    size_t chunk = std::min(sizeof(scratch), static_cast<size_t>(bytes));
    if (rpc_read(conn, scratch, chunk) < 0) break;
    bytes -= chunk;
  }
}

void lupine_dedup_client_populate_from_server(conn_t *conn) {
  if (conn == nullptr || !lupine_dedup_enabled(conn)) {
    return;
  }
  // Only the client side requests the hash list.
  if (conn->dedup_client_cache == nullptr) {
    return;
  }
  LUPINE_TRACE_LOG("LUPINE dedup requesting hash list from server");

  // Send the request: just the op code, no payload.
  if (rpc_write_start_request(conn, LUPINE_RPC_lupineDedupHashList) < 0 ||
      rpc_wait_for_response(conn) < 0) {
    LUPINE_LOG_ERROR("lupine_dedup_client_populate_from_server: request failed");
    return;
  }

  // Read the response: [u32 count][count * 16-byte hashes]
  // Note: rpc_read_start already consumed the invalidations prefix (if any).
  uint32_t count = 0;
  if (rpc_read(conn, &count, sizeof(count)) != sizeof(count)) {
    rpc_read_end(conn);
    LUPINE_LOG_ERROR("lupine_dedup_client_populate_from_server: count read failed");
    return;
  }

  // Sanity bound to keep count * 16 arithmetic safe from overflow and to
  // reject obviously-bogus server responses. 256M entries = 4 GiB of hashes,
  // far above any realistic cache (a 1 TiB compressed cache holds ~524K
  // chunks). On bail we must drain the remaining hash bytes so the next
  // rpc_dispatch() read sees a clean frame boundary.
  if (static_cast<uint64_t>(count) > (256ull * 1024ull * 1024ull)) {
    LUPINE_LOG_ERROR("lupine_dedup_client_populate_from_server: count too large");
    lupine_dedup_drain_remaining(conn,
                                 static_cast<uint64_t>(count) * 16);
    rpc_read_end(conn);
    return;
  }

  auto *mirror = static_cast<lupine_dedup_client_cache *>(
      conn->dedup_client_cache);

  if (count == 0) {
    LUPINE_TRACE_LOG("LUPINE dedup server has 0 cached chunks");
    rpc_read_end(conn);
    return;
  }

  // Stream the hashes in fixed-size batches. This bounds client RAM to
  // kBatchEntries * 16 bytes (64 KiB) regardless of cache size, so a 1 TiB
  // cache with ~524K chunks doesn't require a 8 MiB allocation up front. The
  // mirror is populated incrementally as each batch arrives.
  constexpr uint32_t kBatchEntries = 4096;  // 4096 * 16 = 64 KiB
  std::vector<lupine_dedup_hash128> batch(kBatchEntries);
  uint32_t added = 0;
  bool read_failed = false;
  while (added < count) {
    uint32_t batch_size = std::min(kBatchEntries, count - added);
    size_t batch_bytes =
        static_cast<size_t>(batch_size) * sizeof(lupine_dedup_hash128);
    int n = rpc_read(conn, batch.data(), batch_bytes);
    if (n < 0 || static_cast<size_t>(n) != batch_bytes) {
      read_failed = true;
      break;
    }
    for (uint32_t i = 0; i < batch_size; ++i) {
      lupine_dedup_client_add(mirror, batch[i]);
    }
    added += batch_size;
  }

  if (read_failed) {
    // Drain the unread hash bytes (count - added) * 16 so the connection
    // stays frame-aligned. Without this drain, the next rpc_dispatch() read
    // would interpret hash bytes as a request_id and corrupt the stream.
    lupine_dedup_drain_remaining(
        conn, static_cast<uint64_t>(count - added) * 16);
    rpc_read_end(conn);
    LUPINE_LOG_ERROR("lupine_dedup_client_populate_from_server: hash read failed");
    return;
  }

  LUPINE_TRACE_LOG("LUPINE dedup populated mirror with " << count
                   << " hashes from server");
  rpc_read_end(conn);
}

void lupine_dedup_conn_destroy(conn_t *conn) {
  if (conn == nullptr) return;
  if (conn->dedup_server_cache != nullptr) {
    lupine_dedup_server_destroy(static_cast<lupine_dedup_server_cache *>(
        conn->dedup_server_cache));
    conn->dedup_server_cache = nullptr;
    // Shut down S3 L2 background threads. No-op if S3 was not configured.
    lupine_dedup_s3_destroy();
  }
  if (conn->dedup_client_cache != nullptr) {
    lupine_dedup_client_destroy(static_cast<lupine_dedup_client_cache *>(
        conn->dedup_client_cache));
    conn->dedup_client_cache = nullptr;
  }
}

int lupine_dedup_read_invalidations(conn_t *conn) {
  // Read the dedup prefix that the server wrote at the start of the response.
  //
  // Format:
  //   [u32 inv_count] [inv_count × 16-byte hashes]            (no additions)
  //   [u32 inv_count | 0x80000000] [inv × 16] [u32 add_count] [add × 16]  (with additions)
  //
  // inv_count == 0: no invalidations (common case).
  // inv_count == 0xFFFFFFFF: clear-all marker (all bits set, checked first).
  // Bit 31 set (but not all bits): additions follow after the invalidations.
  auto *mirror = static_cast<lupine_dedup_client_cache *>(
      conn->dedup_client_cache);

  uint32_t inv_count = 0;
  if (rpc_http2_read(conn, &inv_count, sizeof(inv_count)) != sizeof(inv_count)) {
    return -1;
  }

  // Check for clear-all marker first (0xFFFFFFFF has bit 31 set, so we
  // must check it before the bit-31 additions test).
  if (inv_count == 0xFFFFFFFFu) {
    if (mirror != nullptr) {
      lupine_dedup_client_clear(mirror);
    }
    return 0;
  }

  // Check if additions follow (bit 31 set).
  bool has_additions = (inv_count & 0x80000000u) != 0;
  inv_count &= 0x7FFFFFFFu;

  // Read invalidations.
  if (inv_count > 0) {
    if (inv_count > (256u * 1024u * 1024u)) {
      // Sanity bound: 256M entries = 4 GiB of hashes.
      return -1;
    }
    std::vector<lupine_dedup_hash128> invs(inv_count);
    size_t inv_bytes = inv_count * sizeof(lupine_dedup_hash128);
    int read_result = rpc_http2_read(conn, invs.data(), inv_bytes);
    if (read_result < 0 || static_cast<size_t>(read_result) != inv_bytes) {
      return -1;
    }
    if (mirror != nullptr) {
      lupine_dedup_client_invalidate(mirror, invs.data(), inv_count);
    }
  }

  // Read additions (if bit 31 was set).
  if (has_additions) {
    uint32_t add_count = 0;
    if (rpc_http2_read(conn, &add_count, sizeof(add_count)) != sizeof(add_count)) {
      return -1;
    }
    if (add_count > 0) {
      if (add_count > (256u * 1024u * 1024u)) {
        return -1;
      }
      std::vector<lupine_dedup_hash128> adds(add_count);
      size_t add_bytes = add_count * sizeof(lupine_dedup_hash128);
      int read_result = rpc_http2_read(conn, adds.data(), add_bytes);
      if (read_result < 0 || static_cast<size_t>(read_result) != add_bytes) {
        return -1;
      }
      if (mirror != nullptr) {
        for (uint32_t i = 0; i < add_count; ++i) {
          lupine_dedup_client_add(mirror, adds[i]);
        }
      }
    }
  }

  return 0;
}

int lupine_dedup_flush_for_response(conn_t *conn) {
  auto *cache = static_cast<lupine_dedup_server_cache *>(
      conn->dedup_server_cache);
  return lupine_dedup_server_flush_invalidations(cache, conn);
}
