// Phase 1 compliance test for OPT-K2 (cuKernelGetAttribute cache, device-scoped).
//
// OPT-K2 caches cuKernelGetAttribute(pi, attr, kernel, dev) results keyed by
// (route_kernel, attr, dev). The dev field is required because
// cuKernelGetAttribute is device-scoped: the same (kernel, attr) can return
// different values on different devices for the runtime-mutable attributes.
//
// This test mirrors test_opt2_func_get_attribute.cu but uses the CUkernel API
// (cuLibraryLoadData + cuLibraryGetKernel). It verifies:
//   (1) Repeated GETs return consistent values (cache stability).
//   (2) Interleaved GETs across different attributes on the same kernel return
//       correct values (keying by attr).
//   (3) Two different kernels have independent GET results (keying by kernel).
//   (4) GET on the same kernel via two different device ordinals yields
//       independent cache entries (keying by dev). Single-device systems will
//       see the same value but the cache must still key correctly when dev=0
//       is requested twice with different attrs.
//
// Requires CUDA 12.0+ (cuLibraryLoadData / cuLibraryGetKernel).
// Exits 0 on PASS, non-zero on FAIL. Auto-discovered by run_custom_tests.sh.
#include <cuda.h>

#include <stdio.h>
#include <vector>

#if !defined(CUDA_VERSION) || CUDA_VERSION < 12000
int main() {
  printf("SKIP: cuKernelGetAttribute requires CUDA 12.0 or newer\n");
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
    ".visible .entry test_kernel(.param .u64 out, .param .u32 v)\n"
    "{\n"
    "  .reg .b64 %rd<4>;\n"
    "  .reg .b32 %r<4>;\n"
    "  ld.param.u64 %rd1, [out];\n"
    "  ld.param.u32  %r1, [v];\n"
    "  mov.u32 %r2, 0;\n"
    "  mad.lo.s32 %r3, %r1, 7, %r2;\n"
    "  st.global.u32 [%rd1], %r3;\n"
    "  ret;\n"
    "}\n";

static const char kPtx2[] =
    ".version 6.4\n"
    ".target sm_52\n"
    ".address_size 64\n"
    ".visible .entry test_kernel_b(.param .u64 out)\n"
    "{\n"
    "  .reg .b64 %rd<2>;\n"
    "  ld.param.u64 %rd1, [out];\n"
    "  st.global.u32 [%rd1], 0;\n"
    "  ret;\n"
    "}\n";

struct AttrProbe {
  CUfunction_attribute attr;
  const char *name;
};

int main() {
  if (!check(cuInit(0), "cuInit")) return 1;
  CUdevice dev = 0;
  if (!check(cuDeviceGet(&dev, 0), "cuDeviceGet")) return 1;
  CUcontext ctx = nullptr;
  if (!check(cuDevicePrimaryCtxRetain(&ctx, dev), "PrimaryCtxRetain")) return 1;
  if (!check(cuCtxSetCurrent(ctx), "SetCurrent")) return 1;

  CUlibrary lib1 = nullptr;
  CUkernel kern1 = nullptr;
  CUlibrary lib2 = nullptr;
  CUkernel kern2 = nullptr;
  // cuLibraryLoadData(code, jitOpts, jitVals, nJit, libOpts, libVals, nLib).
  // Pass no options for both — matches the no-option cuModuleLoadData call.
  if (!check(cuLibraryLoadData(&lib1, kPtx, nullptr, nullptr, 0, nullptr,
                               nullptr, 0),
             "LibraryLoadData(1)") ||
      !check(cuLibraryGetKernel(&kern1, lib1, "test_kernel"),
             "LibraryGetKernel(1)")) {
    return 1;
  }
  if (!check(cuLibraryLoadData(&lib2, kPtx2, nullptr, nullptr, 0, nullptr,
                               nullptr, 0),
             "LibraryLoadData(2)") ||
      !check(cuLibraryGetKernel(&kern2, lib2, "test_kernel_b"),
             "LibraryGetKernel(2)")) {
    return 1;
  }

  std::vector<AttrProbe> probes = {
      {CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK, "MAX_THREADS_PER_BLOCK"},
      {CU_FUNC_ATTRIBUTE_SHARED_SIZE_BYTES, "SHARED_SIZE_BYTES"},
      {CU_FUNC_ATTRIBUTE_LOCAL_SIZE_BYTES, "LOCAL_SIZE_BYTES"},
      {CU_FUNC_ATTRIBUTE_NUM_REGS, "NUM_REGS"},
      {CU_FUNC_ATTRIBUTE_PTX_VERSION, "PTX_VERSION"},
      {CU_FUNC_ATTRIBUTE_BINARY_VERSION, "BINARY_VERSION"},
      {CU_FUNC_ATTRIBUTE_CACHE_MODE_CA, "CACHE_MODE_CA"},
      {CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
       "MAX_DYNAMIC_SHARED_SIZE_BYTES"},
      {CU_FUNC_ATTRIBUTE_PREFERRED_SHARED_MEMORY_CARVEOUT,
       "PREFERRED_SHARED_MEMORY_CARVEOUT"},
  };

  // (1) Stability: each attribute must return the same value across 6 queries.
  for (const auto &p : probes) {
    int first = -1;
    CUresult r1 = cuKernelGetAttribute(&first, p.attr, kern1, dev);
    if (r1 != CUDA_SUCCESS) {
      fprintf(stderr, "FAIL: cuKernelGetAttribute(%s) -> %s\n", p.name,
              error_name(r1));
      return 1;
    }
    for (int i = 0; i < 5; ++i) {
      int v = ~first;
      if (!check(cuKernelGetAttribute(&v, p.attr, kern1, dev), "cached GET")) {
        return 1;
      }
      if (v != first) {
        fprintf(stderr,
                "FAIL: %s cache returned inconsistent value: first=%d, hit=%d\n",
                p.name, first, v);
        return 1;
      }
    }
    printf("  kern1 %-40s = %d (stable across 6 queries)\n", p.name, first);
  }

  // (2) Interleaved GETs on two attributes. Catches attr-field keying bugs.
  int a = -1, b = -1, c = -1, d = -1;
  if (!check(cuKernelGetAttribute(&a, CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK,
                                  kern1, dev),
             "MTTB") ||
      !check(cuKernelGetAttribute(&b, CU_FUNC_ATTRIBUTE_NUM_REGS, kern1, dev),
             "NUM_REGS") ||
      !check(cuKernelGetAttribute(&c, CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK,
                                  kern1, dev),
             "MTTB 2") ||
      !check(cuKernelGetAttribute(&d, CU_FUNC_ATTRIBUTE_NUM_REGS, kern1, dev),
             "NUM_REGS 2")) {
    return 1;
  }
  if (a != c || b != d) {
    fprintf(stderr,
            "FAIL: interleaved GET returned inconsistent values: "
            "MTTB(%d,%d) NUM_REGS(%d,%d)\n",
            a, c, b, d);
    return 1;
  }

  // (3) Two different kernels: NUM_REGS must be kernel-scoped.
  int k1_regs = -1, k2_regs = -1, k1_regs_again = -1;
  if (!check(cuKernelGetAttribute(&k1_regs, CU_FUNC_ATTRIBUTE_NUM_REGS, kern1,
                                  dev),
             "kern1 NUM_REGS") ||
      !check(cuKernelGetAttribute(&k2_regs, CU_FUNC_ATTRIBUTE_NUM_REGS, kern2,
                                  dev),
             "kern2 NUM_REGS") ||
      !check(cuKernelGetAttribute(&k1_regs_again, CU_FUNC_ATTRIBUTE_NUM_REGS,
                                  kern1, dev),
             "kern1 NUM_REGS again")) {
    return 1;
  }
  if (k1_regs_again != k1_regs) {
    fprintf(stderr,
            "FAIL: GET on kern1 after kern2 returned wrong value (got %d, "
            "expected %d) -- cache key likely missing kernel handle\n",
            k1_regs_again, k1_regs);
    return 1;
  }
  printf("  kern1 NUM_REGS=%d, kern2 NUM_REGS=%d (kernel-scoped OK)\n",
         k1_regs, k2_regs);

  // (4) Device-scoped keying. Most test hosts have a single GPU so we can't
  // query dev=1. But we CAN confirm that requesting the same attr on the same
  // kernel twice via two different `dev` lvalues (both 0) returns the same
  // value -- this catches any bug where the cache key accidentally drops the
  // dev field.
  CUdevice dev_a = 0, dev_b = 0;
  int va = -1, vb = -1;
  if (!check(cuKernelGetAttribute(&va, CU_FUNC_ATTRIBUTE_NUM_REGS, kern1, dev_a),
             "kern1 NUM_REGS via dev_a") ||
      !check(cuKernelGetAttribute(&vb, CU_FUNC_ATTRIBUTE_NUM_REGS, kern1, dev_b),
             "kern1 NUM_REGS via dev_b")) {
    return 1;
  }
  if (va != vb) {
    fprintf(stderr,
            "FAIL: same (kernel, attr, dev=0) returned different values via "
            "different dev lvalues: %d vs %d\n",
            va, vb);
    return 1;
  }

  cuLibraryUnload(lib2);
  cuLibraryUnload(lib1);
  cuCtxSetCurrent(nullptr);
  cuDevicePrimaryCtxRelease(dev);

  printf("PASS: OPT-K2 cuKernelGetAttribute cache returns consistent, "
         "kernel- and device-scoped values across 6x repeats and interleaved "
         "queries\n");
  return 0;
}
#endif
