# Lupine Dedup Architecture

**Status:** Living document. Updated when the implementation changes.
**Last updated:** 2026-07-14 (corrected sharing model)
**Implementation:** `dedup.h`, `dedup.cpp` (server disk cache + client mirror + payload hooks)
**Wire format:** Extensions to the LZ4 block framing in `compress.cpp`
**Default:** OFF. Set `LUPINE_DEDUP=1` on both client and server to enable.

---

## 1. Overview

Lupine's client shim re-sends every byte of every `cuMemcpyHtoD*` call to the server, even when the same bytes were sent seconds ago — by the same client or by a different client. The dedup feature adds a **shared, content-addressed chunk cache** that replaces repeated chunk transfers with a 20-byte hash reference.

**Sharing model (this is the key thing to understand):**

- The **server-side disk cache is shared across all connections and all forked server children**. All children of a server process point at the same `LUPINE_DEDUP_CACHE_DIR` (default `~/.cache/lupine-dedup`). Chunk X uploaded by client A is a HIT for client B on a different connection, because both connections' `disk_cache_impl` instances read and write the same on-disk chunk files. This is a major benefit for multi-tenant serving: two clients loading the same model share the cache.
- The **client-side mirror is per client process** (one `lupine_dedup_client_cache` per client connection). Each client only knows about hashes it has either (a) sent as a MISS, or (b) received from the server's hash-list handshake on connect (the `lupineDedupHashList` RPC, see §9.1). A client does NOT automatically know about chunks other clients cached — but it doesn't need to, because the client only needs to answer "would *my* last send of this hash have been a HIT?".
- The **per-connection state on the server** is just invalidation bookkeeping: `pending_invalidations` (the buffer of evictions to forward to THIS connection's client on the next response), `send_buffer`/`send_count` (the snapshot sent on the next response), and `invalidation_log_offset` (this child's read cursor into the shared `invalidations.log`). Each child has its own `disk_cache_impl` C++ instance for this; the actual chunk data lives on disk and is shared.

So "per-connection" applies to: the `disk_cache_impl` instance, the invalidation stream, and the client mirror. It does **not** apply to the chunks themselves — those are globally shared on disk.

**Scope (v1):** `cuMemcpyHtoD_v2` and `cuMemcpyHtoDAsync_v2` — the client→server host-to-device copy paths. These are the only paths currently marked `COMPRESSIBLE` in `codegen/annotations.h`, so they're the only ones that go through `rpc_write_payload` / `rpc_read_payload_part` (where dedup hooks in).

**Out of scope (future work):**
- `cuModuleLoadData` / `cuLibraryLoadData` (would benefit from the same primitive; they already use `rpc_write_payload`)
- `cuMemcpy2D/3D` host-source paths (currently not even LZ4-framed)
- Server→client direction (DtoH) — not implemented; dedup is client→server only
- Server-side trigger for the `count=0xFFFFFFFF` clear-all marker (the client already handles it; see §3.3)

---

## 2. Design principles

1. **Minimal modification to original files.** All dedup logic lives in `dedup.h` / `dedup.cpp`. The original files (`compress.cpp`, `rpc.cpp`, `h2.cpp`, `rpc.h`) have only dispatch hooks — one `if` block per integration point.

2. **Layered under LZ4, not replacing it.** Dedup adds a DEDUP_REF fast path for cache hits. Cache misses use the existing LZ4 block framing unchanged. A MISS is byte-identical on the wire to what non-dedup code would send.

3. **Default off.** `LUPINE_DEDUP=1` must be set explicitly on both ends. No negotiation — if only one end has it on, the wire format is inconsistent and the connection will fail. This is acceptable for v1 (coordinated deploy).

4. **Disk-backed, shared across all connections.** The server cache stores chunks as files in `~/.cache/lupine-dedup/chunks/` (configurable via `LUPINE_DEDUP_CACHE_DIR`). All forked server children — and therefore all client connections served by this server — share the same cache directory; the filesystem is the shared substrate. A chunk uploaded by client A is a HIT for client B. The OS page cache is the implicit RAM hot tier (no explicit L1 needed). Cache size defaults to 4 GiB, configurable via `LUPINE_DEDUP_CACHE_BYTES`. Cache persists across server restarts.

5. **Client mirror is per client process, hashes only.** Each client process has its own `lupine_dedup_client_cache` mirror (one `unordered_set<hash>`, ~48 bytes per cached chunk). The client doesn't store chunk bytes (it already has them in the caller's buffer), and the mirror exists solely to answer "does the server have this chunk?" without a round-trip. The mirror is **not** shared across client processes — each client only learns about hashes it has sent as MISS or that the server advertised in the connect-time hash-list handshake (see §9.1).

6. **No explicit "cached" ACK.** The client optimistically adds a hash to its mirror after every MISS send. The server always caches misses (except when it can't — see §6). Correctness relies on strict request-response serialization per connection (`call_mutex` in `rpc.cpp`).

---

## 3. Wire format

### 3.1 Existing LZ4 block framing (unchanged)

For payloads ≥ 64 KiB (`kLupineCompressMinBytes`) on a connection that negotiated `x-lupine-compress: lz4`, each 4 MiB block on the wire is:

```
[u32 token] [block bytes]
```

- `token == 0` → block stored raw (LZ4 didn't shrink it)
- `token > 0` → block is `token` bytes of LZ4-compressed data
- The receiver knows each block's uncompressed size from its position in the payload

### 3.2 Dedup extension

A new token value is reserved:

```
LUPINE_DEDUP_REF_TOKEN = 0xFFFFFFFE
```

This value is far above `LZ4_compressBound(4 MiB)` (~4.016 MiB), so it's unambiguous in any block position, including the trailing partial chunk of a payload.

When the token is `LUPINE_DEDUP_REF_TOKEN`, the next 16 bytes are a `lupine_dedup_hash128` (128-bit content hash of the chunk). The receiver looks up the hash in its cache and emits the cached bytes to the destination buffer.

| Case | Wire bytes per 4 MiB chunk | What's sent |
|------|---------------------------|-------------|
| HIT (server has chunk) | 20 | `[0xFFFFFFFE] [16-byte hash]` |
| MISS, LZ4 compresses well | < 4 MiB | `[u32 compressed_size] [LZ4 bytes]` (same as non-dedup) |
| MISS, LZ4 doesn't shrink | 4 MiB + 4 | `[u32 0] [raw 4 MiB]` (same as non-dedup) |
| Below dedup threshold (< 64 KiB) | unchanged | Plain `rpc_write`, neither dedup nor LZ4 |

### 3.3 Invalidation prefix on responses

Every response from a dedup-enabled server carries an invalidation prefix **before** the normal response payload:

```
[u32 count] [count × 16-byte hashes]
```

- `count == 0` — common case, no evictions since the last response (4 bytes overhead)
- `count > 0` — `count` hashes that the server has evicted; the client removes them from its mirror
- `count == 0xFFFFFFFF` — "clear-all" marker. The client already handles this (`lupine_dedup_client_clear`); the server-side trigger (e.g. admin sentinel file) is future work.

The client reads this prefix in `rpc_read_start` (via `lupine_dedup_read_invalidations`) **before** reading any response-specific data. This keeps the client mirror in sync with the server cache state as of the previous response.

**Wire format symmetry:** Only the server side (which has `dedup_server_cache`) writes the prefix. Only the client side (which has `dedup_client_cache`) reads it. The `rpc_write_start_response` and `rpc_read_start` hooks gate on these fields, so non-dedup connections and non-server writes skip the prefix entirely.

### 3.4 No negotiation in v1

Both ends must have `LUPINE_DEDUP=1` set. There's no HTTP/2 header negotiation (unlike `x-lupine-compress: lz4`). If the versions mismatch, the connection will fail with a protocol error on the first dedup-framed payload. This is acceptable for v1; future versions could add an `x-lupine-dedup: v1` header.

---

## 4. Cache data structures

### 4.1 Server disk cache

The server cache is **disk-backed**. Chunks are stored as individual files in a sharded directory structure. The filesystem IS the index — `open()` is the lookup, `unlink()` is the delete. The OS page cache is the implicit RAM hot tier.

**Layout:**
```
<cache_dir>/                            # default: ~/.cache/lupine-dedup
├── chunks/
│   └── ab/cdef0123456789...           # 4 MiB file per chunk, sharded by hash prefix
├── size                                # uint64: current total bytes (fcntl-locked)
└── invalidations.log                   # append-only, 16 bytes per eviction
```

**Per-connection state** (in `disk_cache_impl`, one instance per forked server child):
```c
struct disk_cache_impl {
    std::string cache_dir;
    size_t byte_cap = 0;                        // default 4 GiB
    std::mutex mutex;                           // protects pending_invalidations, send_buffer
    std::vector<lupine_dedup_hash128> pending_invalidations;
    std::vector<lupine_dedup_hash128> send_buffer;   // snapshot for next response
    uint32_t send_count = 0;
    int invalidation_log_fd = -1;               // for reading evictions by other forks
    off_t invalidation_log_offset = 0;          // last read position
};
```

`cache_dir` and `byte_cap` are the same across all instances (read from env at instance creation), so they're effectively per-process constants rather than per-connection state. The genuinely per-connection fields are `pending_invalidations`, `send_buffer`/`send_count`, `invalidation_log_fd`, and `invalidation_log_offset` — these track what THIS connection's client needs to hear about on the next response. Each forked child has its own `disk_cache_impl` because each child serves a different client with a different mirror, but all instances point at the same on-disk cache directory.

There is **no in-process `bytes` counter** on `disk_cache_impl`. The current total cache size lives in the on-disk `size` file and is read freshly on every update via `update_size_file()` (which takes a `fcntl(F_SETLKW)` lock, preads the current value, applies the delta, and writes it back). This is what makes the size tracking correct across forked children that share the cache directory.

**Shared state** (on disk, across all forks):
- `chunks/` — one file per chunk, named by hex-encoded hash, sharded into 256 subdirectories by the first 2 hex chars
- `size` — 8-byte file containing the current total cache size in bytes, protected by `fcntl(F_SETLKW)` for cross-process safety
- `invalidations.log` — append-only log of evicted hashes (16 bytes each), drained by each child on every response

**Eviction policy:** FIFO by file mtime. When a new insert pushes the total over `byte_cap`, the cache is scanned, files are sorted by mtime ascending, and the oldest are `unlink`'d until the total drops to 90% of cap (10% headroom to avoid evicting on every single insert).

**Persistence:** The cache directory is NOT deleted on server shutdown (`lupine_dedup_server_destroy` only closes file descriptors). On restart, existing chunk files are reused — `lupine_dedup_server_insert` uses `O_CREAT|O_EXCL` and skips the write if the file already exists. The `size` file is not bulk-loaded at startup; it is read lazily on the first `update_size_file` call.

### 4.2 Client mirror

```c
struct client_cache_impl {
    std::unordered_set<lupine_dedup_hash128> present;
    std::mutex mutex;
};
```

Hashes only — no chunk bytes. The client already has the bytes in the caller's buffer; the mirror exists solely to answer "does the server have this chunk?" without a round-trip.

---

## 5. Payload hooks

The dedup logic is invoked from `compress.cpp` via three dispatch hooks:

```c
// compress.cpp (3 hooks, ~9 lines total)
int rpc_write_payload(conn_t *conn, const void *data, size_t size) {
  ...
  if (lupine_dedup_enabled(conn)) {
    return lupine_dedup_write_payload(conn, data, size);
  }
  return rpc_write_framed(conn, data, size);
}
```

The actual logic in `dedup.cpp`:

### 5.1 `lupine_dedup_write_payload` (client side)

Walks the source buffer in 4 MiB chunks. For each chunk:
1. Compute 128-bit hash
2. Check client mirror: `lupine_dedup_client_has(hash)`
   - **HIT:** emit `[DEDUP_REF token][hash]` (20 bytes) via `rpc_write`
   - **MISS:** emit LZ4-framed chunk via `rpc_write_framed` (existing path), then `lupine_dedup_client_add(hash)`

**Scratch buffer lifetime:** `rpc_write` stores *pointers* to caller data on the write queue, not copies. The DEDUP_REF token and hash for each HIT are appended to a `thread_local std::deque<std::array<unsigned char, 20>>` (`dedup_write_scratch` in `dedup.cpp`). The deque is cleared at the start of each `lupine_dedup_write_payload` call and survives until the next call on the same thread (always after `rpc_write_end` has flushed the data). `std::deque` is used specifically (rather than `std::vector`) because deque does not relocate existing elements on `emplace_back` — so pointers handed to earlier `rpc_write()` calls in the same payload stay valid. A `std::vector` here would be wrong: it reallocates as it grows, invalidating earlier iovecs.

### 5.2 `lupine_dedup_read_payload_part` (server side)

Reads each chunk's token:
- `LUPINE_DEDUP_REF_TOKEN` → read 16-byte hash, `lupine_dedup_server_lookup(hash, expected_size, dst)`. On hit: memcpy cached bytes to `dst`. On miss (safety net): return -1 (hard failure).
- `token == 0` (raw) → read raw bytes to `dst`, then `lupine_dedup_server_insert(dst, size, &h, nullptr, 0)`
- `token > 0` (LZ4) → read compressed bytes, LZ4-decompress to `dst`, then `lupine_dedup_server_insert(dst, size, &h, compressed_bytes, compressed_size)` so the cache can store the compressed form without recompression

On any MISS path, the server unconditionally inserts the chunk into the cache (unless it can't — see §6).

### 5.3 `lupine_dedup_drain_payload` (server side, discard)

Same token parsing as read, but discards bytes instead of writing to a destination. For DEDUP_REF tokens, reads and discards the 16-byte hash (cached bytes stay on the server, nothing to drain).

---

## 6. Insert failure handling (Option A)

### 6.1 The problem

When `lupine_dedup_server_insert` is called with a chunk it can't cache, the client mirror is in an inconsistent state:

- Client sent the chunk as a MISS → added hash to mirror (optimistic)
- Server couldn't cache → didn't insert a file
- Client mirror says "server has this hash", server doesn't have it
- Next DEDUP_REF for this hash → server lookup misses → **hard failure** (`rpc_read_payload` returns -1)

### 6.2 The fix

When `lupine_dedup_server_insert` can't cache a chunk, it appends the hash to `pending_invalidations` instead of silently returning. The actual code paths that trigger this:

```c
void lupine_dedup_server_insert(...) {
  // (1) chunk_size > byte_cap → too big to ever cache
  if (chunk_size > impl->byte_cap) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending_invalidations.push_back(h);
    return;
  }

  // (2) Can't create the chunk file (disk full, permissions, ...)
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0) {
    if (errno == EEXIST) return;  // already cached, no-op
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending_invalidations.push_back(h);
    return;
  }

  // (3) Write failed (partial write, disk full mid-write, ...)
  if (written < 0 || written != write_size) {
    unlink(path.c_str());
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending_invalidations.push_back(h);
    return;
  }

  // (4) Success: update shared size file, evict oldest if over cap.
  //     Eviction never gives up — it evicts down to 90% of cap. If the
  //     new chunk itself was successfully written, it stays (it's the
  //     newest by mtime, so eviction skips it).
  ...
}
```

### 6.3 The flow

1. Client sends chunk A (MISS) → adds `hash_A` to mirror
2. Server can't cache → appends `hash_A` to `pending_invalidations`
3. Server sends response → flushes `[count=1][hash_A]` as invalidation prefix
4. Client reads invalidation → removes `hash_A` from mirror
5. Next write of chunk A → client mirror doesn't have `hash_A` → sends MISS → works correctly

**Cost:** 20 extra bytes on the affected response (4 bytes count + 16 bytes hash). No extra round-trip. Converts a hard failure into a correct MISS.

### 6.4 When this triggers

- **`chunk_size > byte_cap`** — a single chunk (≤ 4 MiB) larger than the entire cache cap. Only happens if `LUPINE_DEDUP_CACHE_BYTES < 4 MiB` (misconfiguration, or the `too_big_to_cache` test deliberately using a 64 KiB cap).
- **`open()` fails with anything other than `EEXIST`** — disk full, permission denied, ENOSPC, etc.
- **`write()` fails or is short** — disk filled mid-write, I/O error, etc.

What does NOT trigger Option A:
- File already exists (`EEXIST`) — chunk was cached in a previous run or by another fork; the insert is a no-op.
- Eviction — eviction runs after a successful insert and reduces the cache to 90% of cap, but the just-inserted chunk is newest by mtime and survives.

---

## 7. Concurrency model

### 7.1 Per-connection serialization

`rpc_write_start_request` takes `call_mutex` then `write_mutex`. `rpc_write_end` releases both. This means for any given connection, at most one request is being written at a time. The read side mirrors this with `read_cond`.

The end-to-end pattern per connection is strictly alternating:

```
client: write request N → wait for response N → write request N+1 → ...
server: read request N → process → write response N → read request N+1 → ...
```

This serialization is what makes dedup correct without distributed-system heroics: the client's mirror at the moment it writes request N+1 reflects all invalidations the server sent in responses 1..N.

### 7.2 Cache mutex

Both caches have their own `std::mutex`. Lock is held:
- **Client mirror:** per-chunk-operation. `lupine_dedup_client_has` and `lupine_dedup_client_add` each take the lock independently for a single chunk; the lock is NOT held across the whole `lupine_dedup_write_payload` call. This is safe because writes are serialized by `call_mutex` above — only one thread can be writing on this connection at a time, so no other thread can interleave `has`/`add` for the same connection.
- **Server cache:** per-chunk during `lupine_dedup_read_payload_part` (around `pending_invalidations` access), per-flush during `lupine_dedup_flush_for_response`. The disk I/O itself (`open`/`read`/`write`/`unlink`) is NOT under the mutex — only the in-process `pending_invalidations` / `send_buffer` vectors are. Per-chunk locking is fine because lane workers process requests serially per connection.

Neither lock is on the hot LZ4-compression path; it's taken per 4 MiB chunk, not per 16 KiB HTTP/2 frame.

---

## 8. Storage: disk-backed with page cache, optional compression

### 8.1 Design

The server cache stores chunks as **files on disk**. The OS page cache is the implicit RAM hot tier — recently-read files stay in RAM automatically, with no explicit data structure needed. This gives us:

- **Disk capacity** (bounded by disk space, not RAM) — handles 20-40+ GiB models
- **RAM speed for hot chunks** (page cache hit: ~5 µs via `read()`)
- **Persistence across restarts** (chunk files survive server shutdown)
- **Sharing across forks** (all children read/write the same cache directory)

### 8.2 Compression (default: on)

When `LUPINE_DEDUP_COMPRESS_CACHE=1` (the default), chunks are stored **LZ4-compressed** on disk. The compressed bytes come directly from the wire transfer (the client already LZ4-compresses MISS chunks for transmission), so there's no recompression cost on insert. On lookup, the compressed file is read and LZ4-decompressed into the destination buffer.

| Mode | Disk usage | Insert cost | Lookup cost (page cache hot) |
|------|-----------|-------------|------------------------------|
| Compressed (`=1`, default) | ~50% (typical 2:1 ratio for model weights) | ~0 (reuse wire-compressed bytes) | ~8 ms (LZ4 decompress) |
| Uncompressed (`=0`) | 100% (raw 4 MiB per chunk) | ~0 (write raw) | ~5 µs (memcpy from page cache) |

**Compression detection on lookup:** The server detects whether a cached chunk is compressed by comparing the file size to the expected uncompressed chunk size — if they differ, it's compressed; if they match (which only happens when compression was disabled or when LZ4 decided the block was incompressible and stored it raw), it's read directly. This means a single cache directory can contain a mix of compressed and uncompressed files (e.g. if `LUPINE_DEDUP_COMPRESS_CACHE` was changed between runs) and lookups will still work.

**When to use which:**
- **Compressed (default):** disk-constrained environments (Vast.ai, small disks), or when the working set exceeds available disk space. The 8 ms decompression cost per chunk is acceptable for ≤2 Gbps network links (where the alternative is slower network transfer).
- **Uncompressed:** fast networks (≥2 Gbps) with plenty of disk space, where maximum lookup speed matters more than disk usage.

### 8.3 Why not an explicit RAM L1?

An explicit RAM L1 (`unordered_map<hash, vector<char>>` in process memory) would:
- **Duplicate** the page cache (same bytes in both)
- **Compete** with the page cache for RAM
- **Add** ~100 lines of management (LRU, mutex, promotion logic)
- **Not be shared** across forks (unless we use shared memory — the complexity we're avoiding)

The page cache is strictly better: it's free, kernel-managed, shared across all processes, and uses all available RAM rather than a fixed 256 MiB. For a 20 GiB model on a 64 GiB RAM machine, the page cache can hold the entire model in RAM after the first read — an explicit 256 MiB L1 would only hold 1.2% of it.

### 8.4 HDD vs NVMe

The design works with any storage. Hit latency depends on the underlying disk and whether compression is enabled (these are estimates; real numbers depend on hardware):

| Storage | Cold read, uncompressed | Cold read, compressed | Page cache hit, uncompressed | Page cache hit, compressed |
|---------|------------------------|----------------------|------------------------------|---------------------------|
| HDD | ~40 ms | ~28 ms (read 2 MiB + 8 ms decompress) | ~5 µs | ~8 ms |
| SATA SSD | ~8 ms | ~12 ms | ~5 µs | ~8 ms |
| NVMe SSD | ~1.3 ms | ~8.7 ms | ~5 µs | ~8 ms |

For inter-DC network links (50-100 ms RTT, 1 Gbps), even HDD with compression beats re-sending 4 MiB over the network (~83-133 ms). For 10+ Gbps intra-DC links with uncompressed cache, NVMe is needed to break even.

### 8.5 Crash recovery

If the server crashes mid-operation, the cache may have:
- **Ghost entries:** file was deleted but size file not updated — `lupine_dedup_server_lookup` returns false (miss), Option A handles it
- **Orphan files:** chunk file exists but is not referenced — takes up disk space, cleaned by periodic `--rebuild-index` or ignored (a few orphan 4 MiB files are noise)

No WAL, no journaling. The Option A mechanism is the recovery path.

---

## 9. Multi-tenant sharing across connections

This section spells out the cross-connection sharing behavior that falls out of §4's design — the disk cache is shared by construction, not as a separate feature layered on top.

### 9.1 How it works

All forked server children open the same `~/.cache/lupine-dedup/` directory — the filesystem is the shared substrate. No `shm_open`, no `PTHREAD_PROCESS_SHARED`, no shared memory segment. Each child has its own `disk_cache_impl` C++ instance (for per-connection invalidation bookkeeping), but the `cache_dir` field of every instance points at the same path.

```
server.cpp:fork() per connection
  ├── child 1 (disk_cache_impl { cache_dir=~/.cache/lupine-dedup, ... }) ──┐
  ├── child 2 (disk_cache_impl { cache_dir=~/.cache/lupine-dedup, ... }) ──┼──> ~/.cache/lupine-dedup/chunks/  (shared)
  └── child 3 (disk_cache_impl { cache_dir=~/.cache/lupine-dedup, ... }) ──┘    ~/.cache/lupine-dedup/size      (fcntl-locked)
                                                                                 ~/.cache/lupine-dedup/invalidations.log (append-only)
```

Concretely: when client A (served by child 1) uploads chunk X, child 1 calls `lupine_dedup_server_insert`, which writes the file `~/.cache/lupine-dedup/chunks/ab/cdef...`. When client B (served by child 2) later references the same hash — even on its very first upload — child 2 calls `lupine_dedup_server_insert` for the MISS, which opens the same path with `O_CREAT|O_EXCL`, gets `EEXIST`, and returns without writing. B's client then optimistically adds the hash to its mirror (§5.1), so B's *second* upload of X becomes a DEDUP_REF HIT. The first MISS for B is still necessary because B's client mirror starts empty — the mirror is per-client-process, not shared.

**Connect-time hash-list handshake (`lupineDedupHashList` RPC):** to reduce the cost of those first-MISS round-trips after a server restart, the client calls `lupineDedupHashList` immediately after connection setup (see `lupine_dedup_client_populate_from_server` in `client.cpp`). The server handler (`handle_manual_lupineDedupHashList` in `manual_server.cpp`) scans the shared `chunks/` directory, extracts every hash from the file paths, and sends back `[u32 count][count × 16-byte hashes]`. The client streams the response in 4096-hash (64 KiB) batches, adding each batch to its mirror incrementally — client RAM stays at 64 KiB regardless of cache size, so a 1 TiB cache (~524K chunks, ~8 MiB of hashes on the wire) doesn't require an 8 MiB allocation up front. There is no 64K-entry cap on the response; the only sanity bound is 256M entries (4 GiB of hashes), which exists solely to reject obviously-bogus responses and to keep the `count * 16` arithmetic safe from overflow. On any error path after `count` is read (sanity-bound exceeded, or a batch read failed mid-stream), the client drains the remaining hash bytes via `lupine_dedup_drain_remaining` before calling `rpc_read_end` — without this drain, the next `rpc_dispatch` read would interpret unread hash bytes as a `request_id` and corrupt the connection. This means a freshly-connected client gets HITs for *all* chunks any client of this server has ever cached — including chunks cached by other clients in earlier sessions, because the disk cache persists across server restarts. Without this handshake, a client that just connected would have an empty mirror and would MISS on every chunk until it had sent each one at least once; with it, the client benefits from the shared cache state from the very first upload.

### 9.2 Cross-process coordination

| Resource | Mechanism |
|----------|-----------|
| Chunk files | Standard file I/O — multiple processes can read the same file concurrently |
| Size counter | `fcntl(F_SETLKW)` advisory lock on `size` file during read-modify-write |
| Invalidation log | `O_APPEND` is atomic for small writes — any child can append |
| Eviction | Each child independently scans + evicts when over cap (safe — `unlink` is idempotent) |

### 9.3 Cross-process invalidations

When child A evicts a chunk, child B's client mirror is stale. The invalidation log handles this:

1. Child A evicts chunk X → appends `hash_X` to `invalidations.log`
2. Child B's next response → drains the log (reads new entries since last read) → adds `hash_X` to `pending_invalidations`
3. Child B sends `[count=1][hash_X]` as the invalidation prefix on its response
4. Child B's client removes `hash_X` from its mirror
5. Next time child B's client writes chunk X → MISS (correct)

Each child tracks its own read offset (`invalidation_log_offset`) in process memory. On startup, it seeks to the end of the log (only reads new entries).

---

## 10. Configuration

| Env var | Default | Purpose |
|---------|---------|---------|
| `LUPINE_DEDUP` | unset (off) | Set to `1` on both client and server to enable dedup |
| `LUPINE_DEDUP_CACHE_DIR` | `~/.cache/lupine-dedup` | Server disk cache directory. Created if it doesn't exist. Persists across restarts. |
| `LUPINE_DEDUP_CACHE_BYTES` | `4294967296` (4 GiB) | Server cache cap in bytes. Set to at least the model size to avoid eviction churn. |
| `LUPINE_DEDUP_COMPRESS_CACHE` | `1` (on) | When `1`, chunks are stored LZ4-compressed on disk (halving disk usage). When `0`, raw bytes are stored (faster lookups, more disk). |
| `LUPINE_TRACE` | unset | Set to `1` (mapped to stderr to keep stdout clean for device printf capture), `2` (stderr), or a file path to see dedup HIT/MISS trace lines. |
| `LUPINE_LOG_LEVEL` | `debug` | Verbosity for non-trace logs (`none`, `error`, `debug`). |

---

## 11. Testing

### 11.1 Unit tests in `h2_test.cpp`

| Test | Verifies |
|------|----------|
| `test_dedup_payload_round_trip` | Data integrity through dedup path (HIT and MISS) |
| `test_dedup_payload_repeat` | Second identical write is ~22× smaller than first (HIT works) |
| `test_dedup_payload_too_big_to_cache` | Option A: chunk too big to cache doesn't hard-fail on re-write; invalidation flows correctly |
| `test_dedup_disk_cache_persistence` | Chunk files survive a `lupine_dedup_server_destroy` + `lupine_dedup_server_create` cycle on the same cache dir, so the next "restart" write gets a HIT |

**Running the tests:** The tests must be run with `LUPINE_DEDUP=1` in the environment. `h2_test.cpp`'s `make_pair()` calls `rpc_http2_client_init` / `rpc_http2_server_init`, which call `h2_init_direct`, which calls `lupine_dedup_conn_init` — and that function is a no-op unless `LUPINE_DEDUP=1`. Without it, `pair.client.dedup_client_cache` and `pair.server.dedup_server_cache` stay null and the dedup hooks in `compress.cpp` are never taken. Some tests (`too_big_to_cache`, `disk_cache_persistence`) additionally replace `pair.server.dedup_server_cache` with a manually-created cache of a specific byte cap, but the client side still relies on `make_pair` having allocated the mirror.

```bash
LUPINE_DEDUP=1 ./build/h2_test
```

### 11.2 Manual verification

```bash
# On GPU machine (server):
LUPINE_DEDUP=1 LUPINE_TRACE=/tmp/server.log ./build/lupine_driver_server

# On CPU-only machine (client):
LUPINE_DEDUP=1 LUPINE_TRACE=/tmp/client.log \
  LD_PRELOAD=/path/to/build/libcuda.so.1 \
  ./your_cuda_app

# Check trace for dedup lines:
grep "dedup" /tmp/server.log
# Expected on second identical cuMemcpyHtoD:
#   LUPINE dedup read  HIT chunk_size=4194304 chunk_offset=0
```

### 11.3 Test payload note

Tests use 128 KiB payloads (above the 64 KiB LZ4-framing threshold; dedup rides on top of LZ4 framing, so the same threshold applies). 128 KiB is also within the socket buffer, which avoids HTTP/2 flow-control deadlock in the in-process `socketpair` test harness.

The PRNG pattern `(i * 1103515245u + 12345u) & 0xFF` is deterministic and reproducible. It happens to be quite compressible under LZ4 (a 256-byte cycle that LZ4 exploits, taking 128 KiB down to ~800 wire bytes on a MISS) — this is fine because the dedup signal (800 → 36 bytes on the second write) is even more pronounced, so the test still cleanly distinguishes HIT from MISS.

---

## 12. File map

| File | Role | Lines vs upstream |
|------|------|-------------------|
| `dedup.h` | Public API: types, constants, function declarations | +219 (new) |
| `dedup.cpp` | All dedup logic: hash, server cache, client mirror, payload hooks, lifecycle hooks | +932 (new) |
| `compress.cpp` | 3 dispatch hooks (write/read/drain) | +9 |
| `rpc.cpp` | 3 hooks (conn_destroy, read_start, write_start_response) | +13 |
| `h2.cpp` | 1 hook (conn_init) | +1 |
| `rpc.h` | `#include "dedup.h"` + 2 `void *` fields on `conn_t` | +7 |
| `CMakeLists.txt` | xxHash static lib + `dedup.cpp`/`dedup.h` in source/header lists + link `lupine_xxhash` on all 4 targets | +23 |
| `codegen/codegen.py` | Register `lupineDedupHashList` in `PRIVATE_RPC_FUNCTIONS` (survives codegen rerun) | +1 |
| `codegen/gen_api.h` | `#define LUPINE_RPC_lupineDedupHashList 547002917` (regenerated by codegen.py) | +1 |
| `client.cpp` | Call `lupine_dedup_client_populate_from_server` after connect | +5 |
| `manual_server.{cpp,h}` | `handle_manual_lupineDedupHashList` handler + declaration | +30 |
| `server.cpp` | Register handler in `lupine_manual_handlers()` map | +2 |
| `h2_test.cpp` | 4 new test functions + helper + calls in `main()` | +372 |
| `third_party/xxhash/{xxhash.h,xxhash.c,LICENSE}` | Vendored xxHash (BSD-2-Clause) for `XXH3_128bits` | +7306 |
| `docs/dedup-architecture.md` | This document | +new |
| **Total (excl. vendored xxHash)** | | **~1600 lines** |

Original Lupine files (non-dedup, non-vendored): **~90 lines** of changes across 9 files. All actual logic in 2 new files (`dedup.h`, `dedup.cpp`).

---

## 13. Open questions / future work

1. **Wire-format negotiation** — add `x-lupine-dedup: v1` header so mismatched versions degrade gracefully instead of failing
2. **LRU on lookup** — splice chunks to the front of an LRU list on lookup, so eviction is true LRU rather than FIFO-by-mtime. One-line change, no wire-format impact.
3. **Module load dedup** — extend to `cuModuleLoadData` / `cuLibraryLoadData` (they already use `rpc_write_payload`)
4. **Server-side clear-all trigger** — `touch /tmp/lupine-dedup-reset` sentinel file, server sends `count=0xFFFFFFFF` clear-all marker. The client side already handles this via `lupine_dedup_client_clear`; only the server-side trigger is missing.
5. **Per-call opt-out** — `LUPINE_DEDUP_DISABLE_FOR_NEXT_CALL` flag for one-shot uploads
6. **Negotiated DtoH dedup** — server→client direction currently has no dedup; same primitive could be applied in reverse

(Note: items that were previously listed here but are already implemented — disk-backed storage with page cache §8, cross-fork shared cache §9, xxHash-based hashing — have been removed. The clear-all marker on the client side is also already implemented; only the server-side trigger remains.)

---

## 14. Change log

| Date | Change |
|------|--------|
| 2026-07-12 | Initial implementation: per-connection RAM cache, dispatch hooks in original files, default off |
| 2026-07-12 | Option A: insert failure appends to `pending_invalidations` instead of hard-failing on next DEDUP_REF |
| 2026-07-12 | Architecture document created (this file), separating it from the original proposal |
| 2026-07-12 | **Disk cache implementation**: replaced RAM cache with disk-backed file-per-chunk storage. Page cache as implicit RAM tier. Shared across all forks. Persists across restarts. Cross-process invalidation log. Default cache cap raised to 4 GiB. |
| 2026-07-12 | **Hash list on connect (Option 3)**: new `LUPINE_RPC_lupineDedupHashList` RPC. Client requests the server's cached hash list on connection init and pre-populates its mirror. First upload after server restart now gets HITs (previously was all MISS because client mirror was empty). |
| 2026-07-12 | **Disk cache compression**: `LUPINE_DEDUP_COMPRESS_CACHE=1` (default). Chunks stored LZ4-compressed on disk. Compressed bytes reused from wire transfer — no recompression cost on insert. |
| 2026-07-12 | **xxHash**: hashing uses vendored `XXH3_128bits` from `third_party/xxhash` (BSD-2-Clause). SMHasher-certified, ~10-15 GB/s on x86 with AVX2. |
| 2026-07-14 | **Document reconciliation**: corrected this document to match the actual implementation — fixed the `disk_cache_impl` snippet (no `impl->bytes` field; size lives in the on-disk `size` file), removed fabricated `server_entry` OOM failure mode, corrected §6.4 failure-mode list, corrected §7.2 lock scope (per-chunk, not whole-payload), added the missing `test_dedup_disk_cache_persistence` test to §11.1, noted that `LUPINE_DEDUP=1` is required to run tests, corrected line counts in §12, removed already-implemented items from §13 future work, noted that the `count=0xFFFFFFFF` clear-all marker is already handled on the client side, replaced the misleading "250× compression" claim with the actually-observed ~164× for PRNG test data, and corrected §11.3's backwards claim about PRNG compressibility. Also fixed code comments in `dedup.cpp` that referenced a "previous fasthash64-based implementation" and "previous std::vector<unsigned char> implementation" that never existed in this codebase — rephrased as design rationale. |
| 2026-07-14 | **Sharing model correction**: the document previously described the cache as "per-connection", which is misleading — the server-side disk cache is shared across all connections and all forked server children (chunk X uploaded by client A is a HIT for client B), because all `disk_cache_impl` instances point at the same `LUPINE_DEDUP_CACHE_DIR`. Only the `disk_cache_impl` C++ struct, the invalidation stream, and the client mirror are per-connection. Reworded §1, §2.4, §2.5, §4.1, and §9 to reflect this; expanded §9.1 with a concrete cross-client HIT walkthrough and a description of the `lupineDedupHashList` connect-time handshake. Also updated `dedup.h` and `dedup.cpp` header comments to drop the "per-connection" framing. |
| 2026-07-14 | **Streaming hash-list handshake + drain-on-error fix**: removed the 64K-entry cap on `lupineDedupHashList` responses that prevented the handshake from working with caches larger than ~128 GiB compressed (~256 GiB uncompressed). The client now streams the response in 4096-hash (64 KiB) batches, bounding client RAM to 64 KiB regardless of cache size. The sanity bound is now 256M entries (4 GiB of hashes), existing solely to reject obviously-bogus responses and to keep `count * 16` arithmetic safe from overflow. Added `lupine_dedup_drain_remaining` and called it on every error path after `count` is read (sanity-bound exceeded, or batch read failed mid-stream) — without this drain, the next `rpc_dispatch` read would interpret unread hash bytes as a `request_id` and corrupt the connection. This was a latent connection-corruption bug that only manifested when the cache exceeded 64K chunks; below that threshold the cap silently returned without reading the hashes, leaving them on the wire to be misinterpreted by the next dispatch read. |
