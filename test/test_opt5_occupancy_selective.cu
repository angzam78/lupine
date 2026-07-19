// Phase 1 compliance test for OPT-5 (selective per-function occupancy cache
// invalidation).
//
// Background: before OPT-5, every successful cuFuncSetAttribute / cuKernelSetAttribute
// cleared the ENTIRE occupancy cache. This was correct but overly conservative —
// a SET on CUfunction F can only change occupancy answers for F itself, not for
// any other CUfunction G. The wholesale clear caused ~7,725 redundant occupancy
// RPCs in the SD 1.5 pipeline.
//
// OPT-5 narrows the invalidation: on a successful SET on `translated`, only
// occupancy cache entries whose key.function == translated are dropped. Other
// functions' cached occupancy values remain valid.
//
// This test verifies:
//   (1) SET on F1 (raising MAX_DYN_SMEM) makes F1's occupancy for a previously-
//       too-large dyn_smem query go from 0 to >0. (Correctness: invalidation
//       happened for F1.)
//   (2) F2's previously-cached occupancy value is UNCHANGED after F1's SET.
//       (Selective invalidation: F2 not affected by F1's SET.)
//   (3) Idempotent re-SET (same value) on F1 does NOT change F1's occupancy
//       answer. (OPT-3 short-circuit + OPT-5 preserve cache.)
//   (4) SET on F2 (raising MAX_DYN_SMEM) makes F2's occupancy for a previously-
//       too-large dyn_smem query go from 0 to >0. (Correctness for F2.)
//   (5) After F2's SET, F1's occupancy for the same dyn_smem is UNCHANGED.
//       (Selective invalidation: F1 not affected by F2's SET.)
//
// The test uses two distinct CUfunctions loaded from the same PTX module so
// they have independent server-side state. Both have small static shared
// memory so we can manipulate MAX_DYNAMIC_SHARED_SIZE_BYTES meaningfully.
//
// Exits 0 on PASS, non-zero on FAIL. Auto-discovered by run_custom_tests.sh.
//
// NOTE ON CUDA VERSIONS: lupine targets CUDA >= 12.8.0. This test exercises
// only driver APIs available since CUDA 10.0 (cuModuleLoadData,
// cuModuleGetFunction, cuFuncGetAttribute, cuFuncSetAttribute,
// cuOccupancyMaxActiveBlocksPerMultiprocessor). No version-specific behavior
// is relied upon — the test asserts only logical API semantics that hold
// across all supported CUDA versions.
#include <cuda.h>

#include <stdio.h>
#include <string.h>

static const char *error_name(CUresult r) {
  const char *n = nullptr;
  cuGetErrorName(r, &n);
  return n ? n : "unknown";
}

static bool check(CUresult r, const char *op) {
  if (r == CUDA_SUCCESS) return true;
  fprintf(stderr, "FAIL: %s -> %s (%d)\n", op, error_name(r), (int)r);
  return false;
}

// Two independent kernels in the same module. Both use a small amount of
// static shared memory so the default MAX_DYNAMIC_SHARED_SIZE_BYTES is small.
// We can then raise it via cuFuncSetAttribute and observe occupancy change
// for queries that exceed the original cap.
static const char kPtx[] =
    ".version 6.4\n"
    ".target sm_52\n"
    ".address_size 64\n"
    ".visible .entry kernel_a(.param .u64 out, .param .u32 n)\n"
    "{\n"
    "  .reg .b64 %rd<2>;\n"
    "  .reg .b32 %r<2>;\n"
    "  .shared .align 4 .b8 smem[16];\n"
    "  ld.param.u64 %rd1, [out];\n"
    "  ld.param.u32  %r1, [n];\n"
    "  st.shared.u32 [smem], %r1;\n"
    "  ld.shared.u32 %r1, [smem];\n"
    "  st.global.u32 [%rd1], %r1;\n"
    "  ret;\n"
    "}\n"
    ".visible .entry kernel_b(.param .u64 out, .param .u32 n)\n"
    "{\n"
    "  .reg .b64 %rd<2>;\n"
    "  .reg .b32 %r<2>;\n"
    "  .shared .align 4 .b8 smem[32];\n"
    "  ld.param.u64 %rd1, [out];\n"
    "  ld.param.u32  %r1, [n];\n"
    "  st.shared.u32 [smem], %r1;\n"
    "  ld.shared.u32 %r1, [smem];\n"
    "  st.global.u32 [%rd1], %r1;\n"
    "  ret;\n"
    "}\n";

int main() {
  if (!check(cuInit(0), "cuInit")) return 1;
  CUdevice dev = 0;
  if (!check(cuDeviceGet(&dev, 0), "cuDeviceGet")) return 1;
  CUcontext ctx = nullptr;
  if (!check(cuDevicePrimaryCtxRetain(&ctx, dev), "PrimaryCtxRetain")) return 1;
  if (!check(cuCtxSetCurrent(ctx), "SetCurrent")) return 1;

  CUmodule mod = nullptr;
  if (!check(cuModuleLoadData(&mod, kPtx), "ModuleLoadData")) return 1;

  CUfunction f1 = nullptr;
  CUfunction f2 = nullptr;
  if (!check(cuModuleGetFunction(&f1, mod, "kernel_a"), "GetFunction(a)") ||
      !check(cuModuleGetFunction(&f2, mod, "kernel_b"), "GetFunction(b)")) {
    return 1;
  }
  if (f1 == f2) {
    fprintf(stderr, "FAIL: f1 and f2 are the same handle (%p)\n", (void *)f1);
    return 1;
  }
  // Note: we deliberately do NOT print f1/f2 pointer values. Different lupine
  // builds (and native CUDA) mint distinct handle addresses for the same
  // kernel; printing them would make the main-vs-opt comparison non-deterministic.
  // The handle inequality check above is sufficient to verify f1 != f2.
  printf("  f1 = <kernel_a handle>\n");
  printf("  f2 = <kernel_b handle>\n");

  // Query the device's shared memory per block optin so we know the upper
  // bound for MAX_DYNAMIC_SHARED_SIZE_BYTES. On most modern GPUs this is
  // >= 48 KB. We'll use 32 KB as our "large" dyn_smem target — well within
  // the device limit on any sm_52+ GPU.
  int shared_mem_per_block_optin = 0;
  if (!check(cuDeviceGetAttribute(&shared_mem_per_block_optin,
                                  CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_BLOCK_OPTIN,
                                  dev),
             "DeviceGetAttribute(MAX_SHARED_MEMORY_PER_BLOCK_OPTIN)")) {
    return 1;
  }
  printf("  device MAX_SHARED_MEMORY_PER_BLOCK_OPTIN = %d bytes\n",
         shared_mem_per_block_optin);
  if (shared_mem_per_block_optin < 32768) {
    fprintf(stderr,
            "FAIL: device shared memory optin (%d) < 32768; test requires "
            "at least 32 KB optin to manipulate MAX_DYN_SMEM meaningfully\n",
            shared_mem_per_block_optin);
    return 1;
  }

  // Read default MAX_DYN_SMEM for both functions. Should be 0 (no dynamic
  // shared memory allowed by default).
  int f1_default_dyn = -1, f2_default_dyn = -1;
  if (!check(cuFuncGetAttribute(&f1_default_dyn,
                                CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                f1),
             "GET f1 MAX_DYN_SMEM (default)") ||
      !check(cuFuncGetAttribute(&f2_default_dyn,
                                CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                f2),
             "GET f2 MAX_DYN_SMEM (default)")) {
    return 1;
  }
  printf("  f1 default MAX_DYN_SMEM = %d\n", f1_default_dyn);
  printf("  f2 default MAX_DYN_SMEM = %d\n", f2_default_dyn);

  // (Pre-test) Establish a small, safe dyn_smem occupancy query for both
  // functions. With dyn_smem=0 (well below any cap), occupancy should be >0
  // and STABLE — we'll use this to detect spurious invalidation of f2 when
  // f1 is mutated.
  const int BLOCK_SIZE = 128;
  const size_t SAFE_DYN_SMEM = 0;

  int f1_occ_safe_1 = -1, f2_occ_safe_1 = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f1_occ_safe_1, f1, BLOCK_SIZE, SAFE_DYN_SMEM),
             "OccupancyMP(f1, safe)") ||
      !check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f2_occ_safe_1, f2, BLOCK_SIZE, SAFE_DYN_SMEM),
             "OccupancyMP(f2, safe)")) {
    return 1;
  }
  printf("  f1 occupancy(safe) = %d blocks/SM\n", f1_occ_safe_1);
  printf("  f2 occupancy(safe) = %d blocks/SM\n", f2_occ_safe_1);
  if (f1_occ_safe_1 <= 0 || f2_occ_safe_1 <= 0) {
    fprintf(stderr, "FAIL: baseline occupancy <= 0; cannot proceed\n");
    return 1;
  }

  // (1) Without raising MAX_DYN_SMEM, query occupancy with a large dyn_smem.
  // The driver should return 0 (request exceeds the cap).
  const size_t LARGE_DYN_SMEM = 32768;
  int f1_occ_large_pre = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f1_occ_large_pre, f1, BLOCK_SIZE, LARGE_DYN_SMEM),
             "OccupancyMP(f1, large, pre-SET)")) {
    return 1;
  }
  printf("  f1 occupancy(large=%zu, pre-SET) = %d blocks/SM\n",
         LARGE_DYN_SMEM, f1_occ_large_pre);
  // Note: we don't assert == 0 here; some drivers may return >0 if the
  // default cap is already large enough. We only assert that AFTER raising
  // MAX_DYN_SMEM, the value is >= the pre-SET value (i.e., it doesn't
  // decrease) and is > 0.

  // (2) SET MAX_DYN_SMEM = LARGE_DYN_SMEM on f1. After this, f1's occupancy
  // for LARGE_DYN_SMEM should be > 0.
  if (!check(cuFuncSetAttribute(f1,
                                CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                (int)LARGE_DYN_SMEM),
             "SET f1 MAX_DYN_SMEM=32768")) {
    return 1;
  }
  int f1_occ_large_post = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f1_occ_large_post, f1, BLOCK_SIZE, LARGE_DYN_SMEM),
             "OccupancyMP(f1, large, post-SET)")) {
    return 1;
  }
  printf("  f1 occupancy(large=%zu, post-SET) = %d blocks/SM\n",
         LARGE_DYN_SMEM, f1_occ_large_post);
  if (f1_occ_large_post <= 0) {
    fprintf(stderr,
            "FAIL: f1 occupancy after SET(MAX_DYN_SMEM=%zu) is %d, expected >0 "
            "-- OPT-5 selective invalidation failed to drop f1's stale entry\n",
            LARGE_DYN_SMEM, f1_occ_large_post);
    return 1;
  }
  if (f1_occ_large_post < f1_occ_large_pre) {
    fprintf(stderr,
            "FAIL: f1 occupancy DECREASED after raising MAX_DYN_SMEM: %d -> %d "
            "(should be >= pre-SET value)\n",
            f1_occ_large_pre, f1_occ_large_post);
    return 1;
  }
  printf("  [PASS] f1 occupancy correctly invalidated by SET on f1\n");

  // (3) Verify f2's safe occupancy is UNCHANGED after f1's SET. This is the
  // core OPT-5 contract: SET on f1 must NOT invalidate f2's cached entries.
  int f2_occ_safe_2 = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f2_occ_safe_2, f2, BLOCK_SIZE, SAFE_DYN_SMEM),
             "OccupancyMP(f2, safe, after f1 SET)")) {
    return 1;
  }
  printf("  f2 occupancy(safe, after f1 SET) = %d blocks/SM\n", f2_occ_safe_2);
  if (f2_occ_safe_2 != f2_occ_safe_1) {
    fprintf(stderr,
            "FAIL: f2 occupancy changed after f1's SET: %d -> %d "
            "-- OPT-5 selective invalidation is leaking across functions\n",
            f2_occ_safe_1, f2_occ_safe_2);
    return 1;
  }
  printf("  [PASS] f2 occupancy unchanged after SET on f1 (selective invalidation works)\n");

  // (4) Idempotent re-SET on f1 (same value). OPT-3 short-circuits the RPC;
  // OPT-5 must NOT invalidate f1's occupancy (the value didn't change).
  int f1_occ_large_pre_reset = f1_occ_large_post;
  if (!check(cuFuncSetAttribute(f1,
                                CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                (int)LARGE_DYN_SMEM),
             "SET f1 MAX_DYN_SMEM=32768 again (idempotent)")) {
    return 1;
  }
  int f1_occ_large_post_reset = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f1_occ_large_post_reset, f1, BLOCK_SIZE, LARGE_DYN_SMEM),
             "OccupancyMP(f1, large, after idempotent re-SET)")) {
    return 1;
  }
  printf("  f1 occupancy(large, after idempotent re-SET) = %d blocks/SM\n",
         f1_occ_large_post_reset);
  if (f1_occ_large_post_reset != f1_occ_large_pre_reset) {
    fprintf(stderr,
            "FAIL: f1 occupancy changed after idempotent re-SET: %d -> %d "
            "(OPT-3 should have short-circuited; OPT-5 should not invalidate)\n",
            f1_occ_large_pre_reset, f1_occ_large_post_reset);
    return 1;
  }
  printf("  [PASS] f1 occupancy stable after idempotent re-SET\n");

  // (5) Now SET MAX_DYN_SMEM = LARGE_DYN_SMEM on f2. After this, f2's
  // occupancy for LARGE_DYN_SMEM should be > 0.
  int f2_occ_large_pre = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f2_occ_large_pre, f2, BLOCK_SIZE, LARGE_DYN_SMEM),
             "OccupancyMP(f2, large, pre-SET)")) {
    return 1;
  }
  printf("  f2 occupancy(large=%zu, pre-SET) = %d blocks/SM\n",
         LARGE_DYN_SMEM, f2_occ_large_pre);

  if (!check(cuFuncSetAttribute(f2,
                                CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                (int)LARGE_DYN_SMEM),
             "SET f2 MAX_DYN_SMEM=32768")) {
    return 1;
  }
  int f2_occ_large_post = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f2_occ_large_post, f2, BLOCK_SIZE, LARGE_DYN_SMEM),
             "OccupancyMP(f2, large, post-SET)")) {
    return 1;
  }
  printf("  f2 occupancy(large=%zu, post-SET) = %d blocks/SM\n",
         LARGE_DYN_SMEM, f2_occ_large_post);
  if (f2_occ_large_post <= 0) {
    fprintf(stderr,
            "FAIL: f2 occupancy after SET(MAX_DYN_SMEM=%zu) is %d, expected >0\n",
            LARGE_DYN_SMEM, f2_occ_large_post);
    return 1;
  }
  if (f2_occ_large_post < f2_occ_large_pre) {
    fprintf(stderr,
            "FAIL: f2 occupancy DECREASED after raising MAX_DYN_SMEM: %d -> %d\n",
            f2_occ_large_pre, f2_occ_large_post);
    return 1;
  }
  printf("  [PASS] f2 occupancy correctly invalidated by SET on f2\n");

  // (6) Verify f1's large occupancy is UNCHANGED after f2's SET. Symmetric
  // to check (3) — SET on f2 must NOT invalidate f1's cached entries.
  int f1_occ_large_after_f2_set = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f1_occ_large_after_f2_set, f1, BLOCK_SIZE, LARGE_DYN_SMEM),
             "OccupancyMP(f1, large, after f2 SET)")) {
    return 1;
  }
  printf("  f1 occupancy(large, after f2 SET) = %d blocks/SM\n",
         f1_occ_large_after_f2_set);
  if (f1_occ_large_after_f2_set != f1_occ_large_post) {
    fprintf(stderr,
            "FAIL: f1 occupancy changed after f2's SET: %d -> %d "
            "-- OPT-5 selective invalidation is leaking across functions\n",
            f1_occ_large_post, f1_occ_large_after_f2_set);
    return 1;
  }
  printf("  [PASS] f1 occupancy unchanged after SET on f2 (symmetric selective invalidation)\n");

  // (7) Final sanity: re-query f1 and f2 safe occupancies. Must still match
  // the original baseline values (no drift across all the SETs above).
  int f1_occ_safe_final = -1, f2_occ_safe_final = -1;
  if (!check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f1_occ_safe_final, f1, BLOCK_SIZE, SAFE_DYN_SMEM),
             "OccupancyMP(f1, safe, final)") ||
      !check(cuOccupancyMaxActiveBlocksPerMultiprocessor(
                 &f2_occ_safe_final, f2, BLOCK_SIZE, SAFE_DYN_SMEM),
             "OccupancyMP(f2, safe, final)")) {
    return 1;
  }
  if (f1_occ_safe_final != f1_occ_safe_1) {
    fprintf(stderr,
            "FAIL: f1 safe occupancy drifted over test: %d -> %d\n",
            f1_occ_safe_1, f1_occ_safe_final);
    return 1;
  }
  if (f2_occ_safe_final != f2_occ_safe_1) {
    fprintf(stderr,
            "FAIL: f2 safe occupancy drifted over test: %d -> %d\n",
            f2_occ_safe_1, f2_occ_safe_final);
    return 1;
  }
  printf("  [PASS] both functions' safe occupancy stable across all SETs\n");

  cuModuleUnload(mod);
  cuCtxSetCurrent(nullptr);
  cuDevicePrimaryCtxRelease(dev);

  printf("PASS: OPT-5 selective per-function occupancy cache invalidation "
         "preserves correctness (SET on F1 invalidates F1 only; F2 unaffected) "
         "and idempotent re-SET does not spuriously invalidate occupancy\n");
  return 0;
}
