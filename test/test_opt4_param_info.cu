// Phase 1 compliance test for OPT-4 (cuFuncGetParamInfo / cuKernelGetParamInfo
// cached wrappers).
//
// OPT-4 hooks the cached wrappers lupine_cuFuncGetParamInfo_cached and
// lupine_cuKernelGetParamInfo_cached into the cuGetProcAddress_v2 / dlsym
// dispatch tables AND provides public LD_PRELOAD-resolved symbols so PyTorch's
// direct link to libcuda.so.1 hits the cache.
//
// The cache stores (result, offset, size) per (handle, paramIndex, kind). On
// a hit, the cached values are returned without an RPC.
//
// This test verifies:
//   (1) The reported offset/size for each param index match the PTX layout
//       (.u64 first then .u32 -> offset 0 size 8, offset 8 size 4).
//   (2) Repeated GETs return consistent values (cache stability).
//   (3) Out-of-range paramIndex returns CUDA_ERROR_INVALID_VALUE.
//   (4) cuKernelGetParamInfo mirrors cuFuncGetParamInfo for the same kernel.
//
// Exits 0 on PASS, non-zero on FAIL. Auto-discovered by run_custom_tests.sh.
#include <cuda.h>

#include <stdio.h>

#if !defined(CUDA_VERSION) || CUDA_VERSION < 12000
int main() {
  printf("SKIP: cuKernelGetParamInfo requires CUDA 12.0 or newer\n");
  return 0;
}
#else

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

// Kernel with three params of distinct sizes for clear param layout.
//   param 0: .u64 out  -> offset 0,  size 8
//   param 1: .u32 v     -> offset 8,  size 4
//   param 2: .u64 extra -> offset 16, size 8 (aligned)
static const char kPtx[] =
    ".version 6.4\n"
    ".target sm_52\n"
    ".address_size 64\n"
    ".visible .entry three_params(.param .u64 out, .param .u32 v, "
    ".param .u64 extra)\n"
    "{\n"
    "  .reg .b64 %rd<4>;\n"
    "  .reg .b32 %r<2>;\n"
    "  ld.param.u64 %rd1, [out];\n"
    "  ld.param.u32  %r1, [v];\n"
    "  ld.param.u64 %rd2, [extra];\n"
    "  st.global.u32 [%rd1], %r1;\n"
    "  st.global.u64 [%rd2], %rd1;\n"
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
  CUfunction func = nullptr;
  CUlibrary lib = nullptr;
  CUkernel kern = nullptr;
  // cuLibraryLoadData(code, jitOpts, jitVals, nJit, libOpts, libVals, nLib).
  if (!check(cuModuleLoadData(&mod, kPtx), "ModuleLoadData") ||
      !check(cuModuleGetFunction(&func, mod, "three_params"),
             "ModuleGetFunction") ||
      !check(cuLibraryLoadData(&lib, kPtx, nullptr, nullptr, 0, nullptr,
                               nullptr, 0),
             "LibraryLoadData") ||
      !check(cuLibraryGetKernel(&kern, lib, "three_params"),
             "LibraryGetKernel")) {
    return 1;
  }

  // (1) cuFuncGetParamInfo for valid indices. Expected layout matches the PTX
  // .param declarations: .u64 @0, .u32 @8, .u64 @16.
  struct Expected { size_t idx; size_t off; size_t sz; };
  static const Expected kExpected[] = {
      {0, 0, 8},
      {1, 8, 4},
      {2, 16, 8},
  };
  for (const auto &e : kExpected) {
    size_t off = ~0ULL, sz = ~0ULL;
    if (!check(cuFuncGetParamInfo(func, e.idx, &off, &sz),
               "cuFuncGetParamInfo")) {
      return 1;
    }
    if (off != e.off || sz != e.sz) {
      fprintf(stderr,
              "FAIL: cuFuncGetParamInfo(idx=%zu) -> off=%zu sz=%zu, expected off=%zu sz=%zu\n",
              e.idx, off, sz, e.off, e.sz);
      return 1;
    }
    printf("  cuFuncGetParamInfo(idx=%zu) -> off=%zu sz=%zu (OK)\n",
           e.idx, off, sz);
  }

  // (2) Repeat each query 5 times. Cached values must be identical.
  for (const auto &e : kExpected) {
    for (int i = 0; i < 5; ++i) {
      size_t off = ~0ULL, sz = ~0ULL;
      if (!check(cuFuncGetParamInfo(func, e.idx, &off, &sz),
                 "cuFuncGetParamInfo (cached)")) {
        return 1;
      }
      if (off != e.off || sz != e.sz) {
        fprintf(stderr,
                "FAIL: cached cuFuncGetParamInfo(idx=%zu) iter %d -> off=%zu sz=%zu, expected off=%zu sz=%zu\n",
                e.idx, i, off, sz, e.off, e.sz);
        return 1;
      }
    }
  }

  // (3) Out-of-range paramIndex must return CUDA_ERROR_INVALID_VALUE.
  size_t off = 0, sz = 0;
  CUresult oob_rc = cuFuncGetParamInfo(func, 999, &off, &sz);
  if (oob_rc != CUDA_ERROR_INVALID_VALUE) {
    fprintf(stderr,
            "FAIL: cuFuncGetParamInfo(oob=999) returned %s, expected CUDA_ERROR_INVALID_VALUE\n",
            error_name(oob_rc));
    return 1;
  }
  // Repeat the OOB query — must be consistent (caches both result and offset).
  CUresult oob_rc2 = cuFuncGetParamInfo(func, 999, &off, &sz);
  if (oob_rc2 != CUDA_ERROR_INVALID_VALUE) {
    fprintf(stderr,
            "FAIL: cached cuFuncGetParamInfo(oob=999) returned %s, expected CUDA_ERROR_INVALID_VALUE\n",
            error_name(oob_rc2));
    return 1;
  }

  // (4) Same queries via cuKernelGetParamInfo — must return identical values.
  for (const auto &e : kExpected) {
    size_t off = ~0ULL, sz = ~0ULL;
    if (!check(cuKernelGetParamInfo(kern, e.idx, &off, &sz),
               "cuKernelGetParamInfo")) {
      return 1;
    }
    if (off != e.off || sz != e.sz) {
      fprintf(stderr,
              "FAIL: cuKernelGetParamInfo(idx=%zu) -> off=%zu sz=%zu, expected off=%zu sz=%zu\n",
              e.idx, off, sz, e.off, e.sz);
      return 1;
    }
  }

  // (5) Out-of-range via kernel path.
  CUresult oob_kern = cuKernelGetParamInfo(kern, 999, &off, &sz);
  if (oob_kern != CUDA_ERROR_INVALID_VALUE) {
    fprintf(stderr,
            "FAIL: cuKernelGetParamInfo(oob=999) returned %s, expected CUDA_ERROR_INVALID_VALUE\n",
            error_name(oob_kern));
    return 1;
  }

  // (6) Interleaved func/kernel GETs — verify cache keying distinguishes
  // (handle, idx, kind). Fetch idx=0 via func, then idx=1 via kern, then idx=0
  // via func again — the third call must return the same value as the first.
  size_t f_off_0 = ~0ULL, f_sz_0 = ~0ULL;
  size_t k_off_1 = ~0ULL, k_sz_1 = ~0ULL;
  size_t f_off_0_again = ~0ULL, f_sz_0_again = ~0ULL;
  if (!check(cuFuncGetParamInfo(func, 0, &f_off_0, &f_sz_0),
             "interleaved func(0)") ||
      !check(cuKernelGetParamInfo(kern, 1, &k_off_1, &k_sz_1),
             "interleaved kern(1)") ||
      !check(cuFuncGetParamInfo(func, 0, &f_off_0_again, &f_sz_0_again),
             "interleaved func(0) again")) {
    return 1;
  }
  if (f_off_0 != f_off_0_again || f_sz_0 != f_sz_0_again) {
    fprintf(stderr,
            "FAIL: interleaved func(0) inconsistent across calls: off=%zu/%zu sz=%zu/%zu\n",
            f_off_0, f_off_0_again, f_sz_0, f_sz_0_again);
    return 1;
  }
  if (k_off_1 != 8 || k_sz_1 != 4) {
    fprintf(stderr,
            "FAIL: kern(1) returned off=%zu sz=%zu, expected off=8 sz=4\n",
            k_off_1, k_sz_1);
    return 1;
  }

  cuLibraryUnload(lib);
  cuModuleUnload(mod);
  cuCtxSetCurrent(nullptr);
  cuDevicePrimaryCtxRelease(dev);

  printf("PASS: OPT-4 cuFuncGetParamInfo / cuKernelGetParamInfo cache returns "
         "consistent param offsets/sizes, correct out-of-range errors, and "
         "stable values across 5x repeats and interleaved queries\n");
  return 0;
}
#endif
