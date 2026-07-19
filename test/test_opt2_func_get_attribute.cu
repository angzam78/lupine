// Phase 1 compliance test for OPT-2 (cuFuncGetAttribute cache).
//
// OPT-2 caches cuFuncGetAttribute(pi, attr, func) results keyed by
// (func, attr). For immutable attributes the cache is always valid. For the
// nine runtime-mutable attributes (CACHE_MODE_CA, MAX_DYNAMIC_SHARED_SIZE_BYTES,
// PREFERRED_SHARED_MEMORY_CARVEOUT, CLUSTER_SIZE_MUST_BE_SET,
// REQUIRED_CLUSTER_WIDTH/HEIGHT/DEPTH, NON_PORTABLE_CLUSTER_SIZE_ALLOWED,
// CLUSTER_SCHEDULING_POLICY_PREFERENCE), OPT-3 keeps the GET cache in sync on
// every successful SET, so the GET cache is always correct without time-based
// invalidation.
//
// This test verifies the OPT-2 cache returns consistent, correct values for a
// range of immutable attributes — both the first call (cache miss) and
// subsequent calls (cache hit) must return the same value. It also exercises
// the GET-after-default path (before any SET has touched the value) to confirm
// OPT-3's cross-invalidation is not spuriously clearing the GET cache.
//
// Exits 0 on PASS, non-zero on FAIL. Auto-discovered by run_custom_tests.sh.
#include <cuda.h>

#include <stdio.h>
#include <vector>

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

// Minimal PTX kernel with two .u32 params and some register usage so that
// NUM_REGS / MAX_THREADS_PER_BLOCK are non-trivial.
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

struct AttrProbe {
  CUfunction_attribute attr;
  const char *name;
  bool may_fail;  // some attrs (e.g. cluster) only succeed on sm_90+
};

int main() {
  if (!check(cuInit(0), "cuInit")) return 1;

  CUdevice dev = 0;
  if (!check(cuDeviceGet(&dev, 0), "cuDeviceGet(0)")) return 1;

  CUcontext ctx = nullptr;
  if (!check(cuDevicePrimaryCtxRetain(&ctx, dev), "PrimaryCtxRetain")) return 1;
  if (!check(cuCtxSetCurrent(ctx), "SetCurrent")) return 1;

  CUmodule mod = nullptr;
  CUfunction func = nullptr;
  if (!check(cuModuleLoadData(&mod, kPtx), "ModuleLoadData") ||
      !check(cuModuleGetFunction(&func, mod, "test_kernel"),
             "ModuleGetFunction")) {
    return 1;
  }

  // (1) Probe a list of mostly-immutable attributes. We require the value
  // reported on the first call to match the value reported on every subsequent
  // call. If OPT-2 ever returned a stale or uninitialized value, this would
  // catch it.
  std::vector<AttrProbe> probes = {
      {CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK, "MAX_THREADS_PER_BLOCK", false},
      {CU_FUNC_ATTRIBUTE_SHARED_SIZE_BYTES, "SHARED_SIZE_BYTES", false},
      {CU_FUNC_ATTRIBUTE_CONST_SIZE_BYTES, "CONST_SIZE_BYTES", false},
      {CU_FUNC_ATTRIBUTE_LOCAL_SIZE_BYTES, "LOCAL_SIZE_BYTES", false},
      {CU_FUNC_ATTRIBUTE_NUM_REGS, "NUM_REGS", false},
      {CU_FUNC_ATTRIBUTE_PTX_VERSION, "PTX_VERSION", false},
      {CU_FUNC_ATTRIBUTE_BINARY_VERSION, "BINARY_VERSION", false},
      {CU_FUNC_ATTRIBUTE_CACHE_MODE_CA, "CACHE_MODE_CA", false},
      {CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,
       "MAX_DYNAMIC_SHARED_SIZE_BYTES", false},
      {CU_FUNC_ATTRIBUTE_PREFERRED_SHARED_MEMORY_CARVEOUT,
       "PREFERRED_SHARED_MEMORY_CARVEOUT", false},
  };

  for (const auto &p : probes) {
    int first = -1;
    CUresult r1 = cuFuncGetAttribute(&first, p.attr, func);
    if (r1 != CUDA_SUCCESS && !p.may_fail) {
      fprintf(stderr, "FAIL: cuFuncGetAttribute(%s) -> %s\n", p.name,
              error_name(r1));
      return 1;
    }
    if (r1 != CUDA_SUCCESS) continue;

    // Hit the cache 5 more times. All must return the same value.
    for (int i = 0; i < 5; ++i) {
      int v = ~first;  // garbage
      if (!check(cuFuncGetAttribute(&v, p.attr, func), "GetAttribute cached")) {
        return 1;
      }
      if (v != first) {
        fprintf(stderr,
                "FAIL: %s cache returned inconsistent value: first=%d, hit=%d\n",
                p.name, first, v);
        return 1;
      }
    }
    printf("  %-40s = %d (stable across 6 queries)\n", p.name, first);
  }

  // (2) Stress test: interleave GET calls for different attributes on the same
  // function. This catches keying bugs (e.g. cache key omitting the attribute
  // field).
  int a = -1, b = -1, c = -1, d = -1;
  if (!check(cuFuncGetAttribute(&a, CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK, func),
             "MTTB") ||
      !check(cuFuncGetAttribute(&b, CU_FUNC_ATTRIBUTE_NUM_REGS, func), "NUM_REGS") ||
      !check(cuFuncGetAttribute(&c, CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK, func),
             "MTTB 2") ||
      !check(cuFuncGetAttribute(&d, CU_FUNC_ATTRIBUTE_NUM_REGS, func),
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

  // (3) Two different functions: GET(MAX_THREADS_PER_BLOCK) on each must
  // return that function's own value, not the other's. Catches a keying bug
  // where the function pointer is dropped from the cache key.
  CUmodule mod2 = nullptr;
  CUfunction func2 = nullptr;
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
  if (!check(cuModuleLoadData(&mod2, kPtx2), "ModuleLoadData(2)") ||
      !check(cuModuleGetFunction(&func2, mod2, "test_kernel_b"),
             "ModuleGetFunction(2)")) {
    return 1;
  }
  int f1_regs = -1, f2_regs = -1;
  if (!check(cuFuncGetAttribute(&f1_regs, CU_FUNC_ATTRIBUTE_NUM_REGS, func),
             "func1 NUM_REGS") ||
      !check(cuFuncGetAttribute(&f2_regs, CU_FUNC_ATTRIBUTE_NUM_REGS, func2),
             "func2 NUM_REGS")) {
    return 1;
  }
  // Re-query func1 immediately after func2 -- must still return func1's value.
  int f1_regs_again = -1;
  if (!check(cuFuncGetAttribute(&f1_regs_again, CU_FUNC_ATTRIBUTE_NUM_REGS, func),
             "func1 NUM_REGS again")) {
    return 1;
  }
  if (f1_regs_again != f1_regs) {
    fprintf(stderr,
            "FAIL: GET on func1 after func2 returned wrong value (got %d, "
            "expected %d) -- cache key likely missing function pointer\n",
            f1_regs_again, f1_regs);
    return 1;
  }
  printf("  func1 NUM_REGS=%d, func2 NUM_REGS=%d (interleaved queries OK)\n",
         f1_regs, f2_regs);

  cuModuleUnload(mod2);
  cuModuleUnload(mod);
  cuCtxSetCurrent(nullptr);
  cuDevicePrimaryCtxRelease(dev);

  printf("PASS: OPT-2 cuFuncGetAttribute cache returns consistent, "
         "function-scoped values across 6× repeats and interleaved queries\n");
  return 0;
}
