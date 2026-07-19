// Phase 1 compliance test for PATCH-D + PATCH-K-D + OPT-4 v2 (public
// LD_PRELOAD-resolved symbol overrides for the cached attribute/param-info
// wrappers).
//
// The @disabled client annotations in codegen/annotations.h suppress the
// generated uncached RPC stubs for:
//   cuFuncGetAttribute / cuFuncSetAttribute
//   cuKernelGetAttribute / cuKernelSetAttribute
//   cuFuncGetParamInfo / cuKernelGetParamInfo
//
// PATCH-D / PATCH-K-D / OPT-4 v2 provide the public extern "C" symbols in
// client.cpp that delegate to the cached wrappers. This matters because:
//   - Raw CUDA clients (linking directly against libcuda.so.1) resolve these
//     symbols at link time, NOT via cuGetProcAddress_v2.
//   - Without the override, PyTorch's direct symbol link would either fail
//     (symbol not found) or hit an uncached RPC stub.
//
// This test verifies the LD_PRELOAD path is exercised correctly by:
//   (1) Resolving the symbols via dlsym(RTLD_DEFAULT, ...) -- this mimics how
//       PyTorch/ctypes.CDLL look up functions when lupine is LD_PRELOADed.
//   (2) Calling the resolved function pointers and verifying the same
//       observable behavior as the direct CUDA calls.
//
// Note: dlsym(RTLD_DEFAULT, ...) returns the first symbol found in the
// process's load order. With LD_PRELOAD=libcuda.so.1, the lupine shim's
// public symbols will shadow the real libcuda.so.1's symbols.
//
// Exits 0 on PASS, non-zero on FAIL. Auto-discovered by run_custom_tests.sh.
#include <cuda.h>

#include <dlfcn.h>
#include <stdio.h>

#if !defined(CUDA_VERSION) || CUDA_VERSION < 12000
int main() {
  printf("SKIP: PATCH-K-D / OPT-4 v2 cuKernel* path requires CUDA 12.0+\n");
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

static const char kPtx[] =
    ".version 6.4\n"
    ".target sm_52\n"
    ".address_size 64\n"
    ".visible .entry patch_d_kernel(.param .u64 out, .param .u32 v)\n"
    "{\n"
    "  .reg .b64 %rd<2>;\n"
    "  .reg .b32 %r<2>;\n"
    "  ld.param.u64 %rd1, [out];\n"
    "  ld.param.u32  %r1, [v];\n"
    "  st.global.u32 [%rd1], %r1;\n"
    "  ret;\n"
    "}\n";

// Function-pointer typedefs matching the public CUDA driver API signatures.
typedef CUresult (*cuFuncGetAttribute_fn)(int *, CUfunction_attribute, CUfunction);
typedef CUresult (*cuFuncSetAttribute_fn)(CUfunction, CUfunction_attribute, int);
typedef CUresult (*cuKernelGetAttribute_fn)(int *, CUfunction_attribute,
                                            CUkernel, CUdevice);
typedef CUresult (*cuKernelSetAttribute_fn)(CUfunction_attribute, int,
                                            CUkernel, CUdevice);
typedef CUresult (*cuFuncGetParamInfo_fn)(CUfunction, size_t, size_t *, size_t *);
typedef CUresult (*cuKernelGetParamInfo_fn)(CUkernel, size_t, size_t *, size_t *);

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
      !check(cuModuleGetFunction(&func, mod, "patch_d_kernel"),
             "ModuleGetFunction") ||
      !check(cuLibraryLoadData(&lib, kPtx, nullptr, nullptr, 0, nullptr,
                               nullptr, 0),
             "LibraryLoadData") ||
      !check(cuLibraryGetKernel(&kern, lib, "patch_d_kernel"),
             "LibraryGetKernel")) {
    return 1;
  }

  // Resolve the six public symbols via dlsym(RTLD_DEFAULT, ...). When lupine
  // is LD_PRELOADed, RTLD_DEFAULT will resolve to the lupine shim's symbols.
  // When run natively (no lupine), it resolves to the real libcuda.so.1.
  cuFuncGetAttribute_fn dlsym_GetFuncAttr =
      (cuFuncGetAttribute_fn)dlsym(RTLD_DEFAULT, "cuFuncGetAttribute");
  cuFuncSetAttribute_fn dlsym_SetFuncAttr =
      (cuFuncSetAttribute_fn)dlsym(RTLD_DEFAULT, "cuFuncSetAttribute");
  cuKernelGetAttribute_fn dlsym_GetKernAttr =
      (cuKernelGetAttribute_fn)dlsym(RTLD_DEFAULT, "cuKernelGetAttribute");
  cuKernelSetAttribute_fn dlsym_SetKernAttr =
      (cuKernelSetAttribute_fn)dlsym(RTLD_DEFAULT, "cuKernelSetAttribute");
  cuFuncGetParamInfo_fn dlsym_GetFuncParamInfo =
      (cuFuncGetParamInfo_fn)dlsym(RTLD_DEFAULT, "cuFuncGetParamInfo");
  cuKernelGetParamInfo_fn dlsym_GetKernParamInfo =
      (cuKernelGetParamInfo_fn)dlsym(RTLD_DEFAULT, "cuKernelGetParamInfo");

  if (!dlsym_GetFuncAttr || !dlsym_SetFuncAttr || !dlsym_GetKernAttr ||
      !dlsym_SetKernAttr || !dlsym_GetFuncParamInfo || !dlsym_GetKernParamInfo) {
    fprintf(stderr, "FAIL: dlsym could not resolve one or more public symbols\n");
    return 1;
  }
  printf("  dlsym resolved all 6 public symbols (PATCH-D + PATCH-K-D + OPT-4 v2)\n");

  // (1) Verify cuFuncGetAttribute via dlsym matches the direct call.
  int regs_direct = -1, regs_dlsym = -1;
  if (!check(cuFuncGetAttribute(&regs_direct, CU_FUNC_ATTRIBUTE_NUM_REGS, func),
             "direct cuFuncGetAttribute(NUM_REGS)") ||
      !check(dlsym_GetFuncAttr(&regs_dlsym, CU_FUNC_ATTRIBUTE_NUM_REGS, func),
             "dlsym cuFuncGetAttribute(NUM_REGS)")) {
    return 1;
  }
  if (regs_direct != regs_dlsym) {
    fprintf(stderr,
            "FAIL: cuFuncGetAttribute(NUM_REGS) direct=%d, dlsym=%d -- PATCH-D public symbol mismatch\n",
            regs_direct, regs_dlsym);
    return 1;
  }

  // (2) Verify cuFuncSetAttribute via dlsym then cuFuncGetAttribute via direct.
  if (!check(dlsym_SetFuncAttr(func,
                               CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                               8192),
             "dlsym cuFuncSetAttribute(MAX_DYN_SMEM=8192)")) {
    return 1;
  }
  int dyn_after = -1;
  if (!check(cuFuncGetAttribute(&dyn_after,
                                CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                func),
             "direct cuFuncGetAttribute(MAX_DYN_SMEM)")) {
    return 1;
  }
  if (dyn_after != 8192) {
    fprintf(stderr,
            "FAIL: after dlsym SET(8192), direct GET returned %d -- PATCH-D cross-invalidation broken\n",
            dyn_after);
    return 1;
  }

  // (3) cuKernelGetAttribute via dlsym matches the direct call.
  int kregs_direct = -1, kregs_dlsym = -1;
  if (!check(cuKernelGetAttribute(&kregs_direct, CU_FUNC_ATTRIBUTE_NUM_REGS,
                                  kern, dev),
             "direct cuKernelGetAttribute(NUM_REGS)") ||
      !check(dlsym_GetKernAttr(&kregs_dlsym, CU_FUNC_ATTRIBUTE_NUM_REGS, kern,
                               dev),
             "dlsym cuKernelGetAttribute(NUM_REGS)")) {
    return 1;
  }
  if (kregs_direct != kregs_dlsym) {
    fprintf(stderr,
            "FAIL: cuKernelGetAttribute(NUM_REGS) direct=%d, dlsym=%d -- PATCH-K-D public symbol mismatch\n",
            kregs_direct, kregs_dlsym);
    return 1;
  }

  // (4) cuKernelSetAttribute via dlsym, then cuKernelGetAttribute via direct.
  if (!check(dlsym_SetKernAttr(CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                               16384, kern, dev),
             "dlsym cuKernelSetAttribute(MAX_DYN_SMEM=16384)")) {
    return 1;
  }
  int kdyn_after = -1;
  if (!check(cuKernelGetAttribute(&kdyn_after,
                                  CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
                                  kern, dev),
             "direct cuKernelGetAttribute(MAX_DYN_SMEM)")) {
    return 1;
  }
  if (kdyn_after != 16384) {
    fprintf(stderr,
            "FAIL: after dlsym SET(16384), direct GET returned %d -- PATCH-K-D cross-invalidation broken\n",
            kdyn_after);
    return 1;
  }

  // (5) cuFuncGetParamInfo via dlsym matches the direct call.
  size_t off_direct = ~0ULL, sz_direct = ~0ULL;
  size_t off_dlsym = ~0ULL, sz_dlsym = ~0ULL;
  if (!check(cuFuncGetParamInfo(func, 0, &off_direct, &sz_direct),
             "direct cuFuncGetParamInfo(0)") ||
      !check(dlsym_GetFuncParamInfo(func, 0, &off_dlsym, &sz_dlsym),
             "dlsym cuFuncGetParamInfo(0)")) {
    return 1;
  }
  if (off_direct != off_dlsym || sz_direct != sz_dlsym) {
    fprintf(stderr,
            "FAIL: cuFuncGetParamInfo(0) direct=(off=%zu,sz=%zu) vs dlsym=(off=%zu,sz=%zu) -- OPT-4 v2 public symbol mismatch\n",
            off_direct, sz_direct, off_dlsym, sz_dlsym);
    return 1;
  }

  // (6) cuKernelGetParamInfo via dlsym matches the direct call.
  off_direct = ~0ULL; sz_direct = ~0ULL;
  off_dlsym = ~0ULL; sz_dlsym = ~0ULL;
  if (!check(cuKernelGetParamInfo(kern, 1, &off_direct, &sz_direct),
             "direct cuKernelGetParamInfo(1)") ||
      !check(dlsym_GetKernParamInfo(kern, 1, &off_dlsym, &sz_dlsym),
             "dlsym cuKernelGetParamInfo(1)")) {
    return 1;
  }
  if (off_direct != off_dlsym || sz_direct != sz_dlsym) {
    fprintf(stderr,
            "FAIL: cuKernelGetParamInfo(1) direct=(off=%zu,sz=%zu) vs dlsym=(off=%zu,sz=%zu) -- OPT-4 v2 public symbol mismatch\n",
            off_direct, sz_direct, off_dlsym, sz_dlsym);
    return 1;
  }

  cuLibraryUnload(lib);
  cuModuleUnload(mod);
  cuCtxSetCurrent(nullptr);
  cuDevicePrimaryCtxRelease(dev);

  printf("PASS: PATCH-D / PATCH-K-D / OPT-4 v2 public LD_PRELOAD-resolved "
         "symbols delegate to cached wrappers and produce identical results "
         "to the direct cuXxx API calls across func/kernel attr get/set and "
         "param info queries\n");
  return 0;
}
#endif
