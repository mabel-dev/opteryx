import os
import sys
import subprocess
import pytest

try:
    from opteryx.compiled import simd_probe
except Exception:  # compiled extension not built/available
    simd_probe = None


def test_cpu_probe_basic():
    # basic sanity checks for probe wrappers
    if simd_probe is None:
        pytest.skip("compiled simd_probe extension not available")
    assert isinstance(simd_probe.cpu_supports_avx2(), bool)
    assert isinstance(simd_probe.cpu_supports_neon(), bool)


def _run_check_with_env(env_vars):
    env = os.environ.copy()
    env.update(env_vars)
    code = "from opteryx.compiled import simd_probe; simd_probe.check_env_or_abort(); print('ok')"
    p = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    return p


def test_fail_if_not_avx2_behavior():
    if simd_probe is None:
        pytest.skip("compiled simd_probe extension not available")
    supports = simd_probe.cpu_supports_avx2()
    p = _run_check_with_env({"OPTERYX_FAIL_IF_NOT_AVX2": "1"})
    if supports:
        assert p.returncode == 0
        assert "ok" in p.stdout
    else:
        # should abort / non-zero exit
        assert p.returncode != 0
