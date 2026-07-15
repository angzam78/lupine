#ifndef LUPINE_DEDUP_H
#define LUPINE_DEDUP_H

// Content-addressed chunk cache for client->server payload deduplication.
// See docs/dedup-architecture.md for the full design.
//
// The cache sits underneath the existing LZ4 block framing in compress.cpp:
// each 4 MiB chunk of a payload is hashed, and chunks the server already has
// are replaced on the wire by a 4-byte DEDUP_REF token + 16-byte hash. The
// server stores chunks as files on disk (the filesystem IS the index); the
// client keeps a mirror set of (hash) so it can decide hit-vs-miss without
// a round-trip. Server evictions are piggybacked on the next RPC response
// so the client mirror stays in sync.
//
// Sharing model: the server-side disk cache is SHARED across all connections
// and all forked server children (they all point at the same
// LUPINE_DEDUP_CACHE_DIR). A chunk uploaded by client A is a HIT for client
// B on a different connection. Per-connection state is limited to: the
// disk_cache_impl C++ instance (per forked child), the invalidation stream
// (each client has its own mirror to keep in sync), and the client mirror
// itself (one per client process).

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>  // for std::hash primary template (specialized below)
#include <vector>      // for lupine_dedup_server_scan_hashes output param

struct conn_t;

// 128-bit content hash for a chunk.
struct lupine_dedup_hash128 {
    uint8_t bytes[16];
    bool operator==(const lupine_dedup_hash128 &o) const {
        return std::memcmp(bytes, o.bytes, 16) == 0;
    }
    bool operator!=(const lupine_dedup_hash128 &o) const {
        return !(*this == o);
    }
};

namespace std {
template <> struct hash<lupine_dedup_hash128> {
    size_t operator()(const lupine_dedup_hash128 &h) const noexcept {
        // The input is already a 128-bit hash, so we just fold 16 bytes down
        // to 8 for the bucket index. Quality of this hash doesn't matter much
        // because the keys are already uniformly distributed.
        uint64_t x;
        std::memcpy(&x, h.bytes, sizeof(x));
        return std::hash<uint64_t>{}(x);
    }
};
} // namespace std

// Wire-format constant. A token value of 0xFFFFFFFE in the LZ4 block stream
// means "dedup reference": the next 16 bytes are a lupine_dedup_hash128 that
// the receiver looks up in its cache. The value is far above
// LZ4_compressBound(4 MiB), so it is unambiguous in any block position.
constexpr uint32_t LUPINE_DEDUP_REF_TOKEN = 0xFFFFFFFEu;

// Computes a 128-bit hash of `len` bytes at `data` using XXH3_128bits from
// vendored xxHash (third_party/xxhash, BSD-2-Clause). Runs at ~10-15 GB/s
// on x86 with AVX2, formally analyzed (SMHasher-certified).
void lupine_dedup_hash(const void *data, size_t len,
                       lupine_dedup_hash128 *out);

// Returns 1 if dedup is enabled for this process (LUPINE_DEDUP=1 env var,
// default OFF), 0 otherwise. Both client and server must have it enabled
// for the wire format to be consistent -- there is no negotiation in v1.
int lupine_dedup_enabled_globally();

// Returns 1 if dedup is enabled for this connection (global flag is on AND
// the connection has its cache allocated), 0 otherwise.
int lupine_dedup_enabled(conn_t *conn);

// Default server cache cap in bytes (4 GiB = 1024 chunks of 4 MiB).
// Set LUPINE_DEDUP_CACHE_BYTES to override. For large models (20+ GiB),
// set this to at least the model size to avoid eviction churn.
size_t lupine_dedup_server_byte_cap_default();

// Returns 1 if disk cache compression is enabled (LUPINE_DEDUP_COMPRESS_CACHE
// env var, default 1 = on). When enabled, chunks are stored LZ4-compressed
// on disk (halving disk usage) and decompressed on lookup (~8 ms per 4 MiB
// chunk). Set to 0 to store raw bytes (faster lookups, more disk usage).
int lupine_dedup_compress_cache_enabled();

// ---------------------------------------------------------------------------
// Server-side disk cache.
//
// Chunks are stored as files in <cache_dir>/chunks/ (sharded by hash prefix).
// The filesystem IS the index. The OS page cache is the implicit RAM tier.
// Cache persists across server restarts. Shared across all forked children.
//
// Cache directory is set by LUPINE_DEDUP_CACHE_DIR env var, defaulting to
// ~/.cache/lupine-dedup. The directory is created if it doesn't exist.
// ---------------------------------------------------------------------------

struct lupine_dedup_server_cache;

// Creates or opens a server disk cache with the given byte capacity.
// The cache directory (from LUPINE_DEDUP_CACHE_DIR or ~/.cache/lupine-dedup)
// is created if it doesn't exist. Existing chunk files from a previous run
// are preserved and reused.
lupine_dedup_server_cache *lupine_dedup_server_create(size_t byte_cap);
// Closes file descriptors but does NOT delete the cache directory. The disk
// cache persists across server restarts.
void lupine_dedup_server_destroy(lupine_dedup_server_cache *c);

// Called after the server decodes a missed chunk. Hashes the chunk, writes
// it to disk (if not already present), and handles eviction if the cache
// exceeds its byte cap. If the chunk can't be cached (too large, disk full),
// the hash is appended to pending_invalidations (Option A). Returns the
// hash by reference.
//
// `compressed_data` / `compressed_size`: if non-null and compression is
// enabled (LUPINE_DEDUP_COMPRESS_CACHE=1, the default), the compressed
// bytes are stored on disk directly (avoiding recompression). If null or
// compression is disabled, the raw `chunk` bytes are stored.
void lupine_dedup_server_insert(lupine_dedup_server_cache *c,
                                const void *chunk, size_t chunk_size,
                                lupine_dedup_hash128 *out_hash,
                                const void *compressed_data = nullptr,
                                size_t compressed_size = 0);

// Called when the server sees a DEDUP_REF token. On hit: reads the cached
// chunk from disk into `dst` and returns true. On miss (should not happen
// given the client mirror, but is a safety net): returns false.
// `expected_chunk_size` is the chunk size implied by the payload position;
// a short read (hash collision, astronomically unlikely with 128-bit hashes)
// is treated as a miss. If the cache is compressed (LUPINE_DEDUP_COMPRESS_CACHE=1),
// the file is read and LZ4-decompressed into `dst`.
bool lupine_dedup_server_lookup(lupine_dedup_server_cache *c,
                                const lupine_dedup_hash128 &hash,
                                size_t expected_chunk_size, void *dst);

// Scans the disk cache and collects all cached chunk hashes. Used by
// handle_manual_lupineDedupHashList to send the hash list to a newly-
// connected client so it can populate its mirror. The client then gets
// HITs on the first upload after a server restart (the disk cache
// persists, and the client mirror is pre-populated from the server's
// state rather than from send history).
void lupine_dedup_server_scan_hashes(lupine_dedup_server_cache *c,
                                     std::vector<lupine_dedup_hash128> *out);

// Drains the pending-invalidation list and writes it to the response as a
// [u32 count][count * 16 bytes] prefix. Called by rpc_write_start_response.
// After this call the pending list is empty. Returns 0 on success, -1 on
// write failure.
int lupine_dedup_server_flush_invalidations(lupine_dedup_server_cache *c,
                                            conn_t *conn);

// ---------------------------------------------------------------------------
// Client-side mirror.
// ---------------------------------------------------------------------------

struct lupine_dedup_client_cache;

lupine_dedup_client_cache *lupine_dedup_client_create();
void lupine_dedup_client_destroy(lupine_dedup_client_cache *c);

// True if the mirror believes the server has this hash cached.
bool lupine_dedup_client_has(lupine_dedup_client_cache *c,
                             const lupine_dedup_hash128 &hash);

// Records that the server now has this hash (called after a miss is sent).
void lupine_dedup_client_add(lupine_dedup_client_cache *c,
                             const lupine_dedup_hash128 &hash);

// Removes each hash in `hashes` from the mirror (called after the server
// sends invalidations).
void lupine_dedup_client_invalidate(lupine_dedup_client_cache *c,
                                    const lupine_dedup_hash128 *hashes,
                                    size_t count);

// Empties the entire mirror (called on connection reset or hard error).
void lupine_dedup_client_clear(lupine_dedup_client_cache *c);

// ---------------------------------------------------------------------------
// Payload hooks -- called from compress.cpp when dedup is enabled.
// These contain the full per-chunk dedup + LZ4 logic; compress.cpp's
// rpc_write_payload / rpc_read_payload_part / rpc_drain_payload just
// dispatch to these when lupine_dedup_enabled(conn) returns true.
// ---------------------------------------------------------------------------

// Writes a bulk data payload with dedup. Each 4 MiB chunk is hashed; on a
// cache hit the chunk is replaced by [DEDUP_REF token][hash] (20 bytes),
// on a miss the chunk is sent via rpc_write_framed (existing LZ4 path) and
// the hash is added to the client mirror.
int lupine_dedup_write_payload(conn_t *conn, const void *data, size_t size);

// Reads `size` bytes of a dedup-framed payload. Handles DEDUP_REF tokens
// (cache lookup) and LZ4-compressed/raw blocks (cache insert on miss).
int lupine_dedup_read_payload_part(conn_t *conn, void *data, size_t size);

// Drains `size` bytes of a dedup-framed payload (discard).
int lupine_dedup_drain_payload(conn_t *conn, size_t size);

// ---------------------------------------------------------------------------
// Connection lifecycle hooks -- called from rpc.cpp / h2.cpp.
// ---------------------------------------------------------------------------

// Allocates the per-connection dedup state. Called from h2_init_direct.
// `is_server` = 1 -> create a server disk_cache_impl instance (the on-disk
// chunks are shared across all instances via LUPINE_DEDUP_CACHE_DIR; only
// the invalidation bookkeeping in the instance is per-connection).
// `is_server` = 0 -> create a client mirror (always per-client-process).
// No-op if LUPINE_DEDUP is not set to "1" (globally disabled).
void lupine_dedup_conn_init(conn_t *conn, int is_server);

// Client-side: requests the hash list from the server and populates the
// client mirror. Called after connection init so the first upload after a
// server restart gets HITs (the mirror reflects the server's disk cache
// state, not the client's send history). No-op if dedup is disabled or
// this is a server-side connection.
void lupine_dedup_client_populate_from_server(conn_t *conn);

// Frees whichever cache exists on the connection. Called from rpc_conn_destroy.
void lupine_dedup_conn_destroy(conn_t *conn);

// Reads the dedup invalidations prefix from a response. Called from
// rpc_read_start when the connection has a client mirror. Returns 0 on
// success, -1 on read failure.
int lupine_dedup_read_invalidations(conn_t *conn);

// Flushes pending invalidations AND additions as the first bytes of a
// response. Called from rpc_write_start_response when the connection has
// a server cache.
//
// Wire format (when additions are present):
//   [u32 inv_count | 0x80000000] [u32 add_count] [inv × 16] [add × 16]
// When no additions:
//   [u32 inv_count] [inv × 16]
// Bit 31 of inv_count signals "additions follow." Real inv_count values
// never exceed 256M, so bit 31 is always available. The 0xFFFFFFFF
// clear-all marker is checked before the bit-31 test (it has all bits set).
//
// add_count is capped at 4096 per response to bound the prefix size.
// Remaining additions are sent on subsequent responses.
//
// Returns 0 on success, -1 on write failure.
int lupine_dedup_flush_for_response(conn_t *conn);

// Appends a hash to the shared additions log. Called after a successful
// local-disk insert to notify other forked children (and their clients)
// that this chunk is now cached.
void lupine_dedup_server_append_addition(
    const lupine_dedup_hash128 &hash);

// Server-side RPC handler for lupineDedupHashList. Scans the L1 disk cache
// and the L2 S3 manifest, sends the union of hashes to the client so it can
// pre-populate its mirror. Implementation lives in dedup.cpp.
int handle_manual_lupineDedupHashList(conn_t *conn);

#endif
