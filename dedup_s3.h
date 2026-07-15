#ifndef LUPINE_DEDUP_S3_H
#define LUPINE_DEDUP_S3_H

// S3-compatible L2 cache for the dedup system. See docs/dedup-architecture.md.
//
// When LUPINE_DEDUP_S3_BUCKET is set (and LUPINE_DEDUP=1), chunks evicted
// from the local disk L1 cache are retained in S3-compatible object storage
// (B2, R2, AWS S3, MinIO, etc.). On an L1 miss, the server checks S3 before
// returning a miss to the client. S3 chunks are promoted to L1 on read.
//
// S3 is best-effort: if the S3 endpoint is unreachable or credentials are
// invalid, L2 is silently disabled and the server operates as L1-only. A
// background thread periodically retries the connection and re-enables L2
// when S3 becomes available.
//
// New S3 chunks (from async write-through or cross-server sharing) are
// pushed to clients via the additions channel (bit 31 of inv_count in the
// response prefix, see dedup.cpp).

#include "dedup.h"
#include <vector>

// Returns 1 if S3 L2 is configured (LUPINE_DEDUP_S3_BUCKET is set and
// LUPINE_DEDUP=1), 0 otherwise. Cached at first call.
int lupine_dedup_s3_configured();

// Returns 1 if S3 L2 is available (configured AND the initial test request
// succeeded). Transitions to 0 on sustained failures and back to 1 when
// the background retry succeeds.
int lupine_dedup_s3_available();

// Per-child lifecycle. Init loads the manifest from S3 (or starts with an
// empty manifest if S3 is unavailable) and starts the background
// write-through thread. Destroy stops background threads and flushes
// pending writes. No-op if S3 is not configured.
void lupine_dedup_s3_init();
void lupine_dedup_s3_destroy();

// L2 lookup. Fetches the chunk from S3, decompresses if needed, writes
// raw bytes to dst. Returns true on hit, false on miss or error.
// Does NOT promote to L1 — the caller handles promotion by calling
// lupine_dedup_server_insert after a successful lookup.
bool lupine_dedup_s3_lookup(const lupine_dedup_hash128 &hash,
                             size_t expected_size, void *dst);

// Async S3 PUT. Enqueues the chunk for background upload. Non-blocking.
// The data is copied into the queue entry, so the caller's buffer can be
// freed immediately after this call returns. If the queue is full, the
// entry is dropped (best-effort — the chunk is still in L1).
void lupine_dedup_s3_insert_async(const lupine_dedup_hash128 &hash,
                                   const void *data, size_t size,
                                   const void *compressed_data,
                                   size_t compressed_size);

// Returns all S3-cached hashes from the in-memory manifest. Used by
// the handshake handler (handle_manual_lupineDedupHashList) to include
// S3 hashes in the connect-time response, so clients get HITs for
// S3-cached chunks from the very first upload.
void lupine_dedup_s3_scan_hashes(std::vector<lupine_dedup_hash128> *out);

// Drains new S3 hashes (added since the last call) for the additions
// channel. Called from lupine_dedup_server_flush_invalidations so that
// new S3 hashes are piggybacked on the next response to this client.
// The output vector is appended to (not cleared).
void lupine_dedup_s3_drain_additions(
    std::vector<lupine_dedup_hash128> *out);

// Returns true if the hash is in the S3 manifest (upload completed).
// Used by the L1 eviction logic to avoid evicting chunks that haven't
// been uploaded to S3 yet. If S3 is not available, always returns true
// (don't block eviction when there's no L2 safety net).
int lupine_dedup_s3_has_hash(const lupine_dedup_hash128 &hash);

// Cleanup: deletes oldest S3 chunks until total stored size <= byte_cap.
// Reads the in-memory manifest (which includes timestamps and sizes),
// sorts by timestamp ascending, batch-deletes the oldest, and rewrites
// the manifest. Called on server startup and on last-client-disconnect.
// Returns 0 on success, -1 on error.
int lupine_dedup_s3_cleanup(size_t byte_cap);

#endif // LUPINE_DEDUP_S3_H
