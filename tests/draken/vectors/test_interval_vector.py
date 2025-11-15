"""IntervalVector tests covering Arrow interop and normalization."""

import pytest
import pyarrow as pa

from opteryx.draken import Vector

MICROSECONDS_PER_DAY = 24 * 60 * 60 * 1_000_000
_MONTH_INTERVAL_FACTORY = getattr(pa, "month_interval", None)
_DAY_TIME_INTERVAL_FACTORY = getattr(pa, "day_time_interval", None)
MONTH_INTERVAL_TYPE = _MONTH_INTERVAL_FACTORY() if _MONTH_INTERVAL_FACTORY else None
DAY_TIME_INTERVAL_TYPE = _DAY_TIME_INTERVAL_FACTORY() if _DAY_TIME_INTERVAL_FACTORY else None


def _is_interval_vector(vec):
    return vec.__class__.__name__ == "IntervalVector"


def _build_month_day_nano():
    return pa.array(
        [
            (2, 0, 0),
            (0, 1, 1_500_000_000),
            None,
        ],
        type=pa.month_day_nano_interval(),
    )


def test_from_arrow_month_day_nano():
    arr = _build_month_day_nano()
    vec = Vector.from_arrow(arr)

    assert _is_interval_vector(vec)
    assert vec.to_pylist() == [
        (2, 0),
        (0, MICROSECONDS_PER_DAY + 1_500_000),
        None,
    ]


def test_to_arrow_interval_roundtrip():
    arr = _build_month_day_nano()
    vec = Vector.from_arrow(arr)

    rebuilt = vec.to_arrow_interval()
    assert rebuilt.equals(arr)


def test_fixed_size_binary_roundtrip():
    arr = _build_month_day_nano()
    vec = Vector.from_arrow(arr)

    binary = vec.to_arrow_binary()
    assert pa.types.is_fixed_size_binary(binary.type)

    vec2 = Vector.from_arrow(binary)
    assert _is_interval_vector(vec2)
    assert vec2.to_pylist() == vec.to_pylist()


@pytest.mark.skipif(MONTH_INTERVAL_TYPE is None, reason="PyArrow build lacks month interval type")
def test_month_interval_normalization():
    arr = pa.array([3, None, -1], type=MONTH_INTERVAL_TYPE)
    vec = Vector.from_arrow(arr)

    assert vec.to_pylist() == [(3, 0), None, (-1, 0)]


@pytest.mark.skipif(DAY_TIME_INTERVAL_TYPE is None, reason="PyArrow build lacks day_time interval type")
def test_day_time_interval_normalization():
    arr = pa.array(
        [
            (0, 5_000),  # 5 seconds
            (1, 0),      # 1 day
            None,
        ],
        type=DAY_TIME_INTERVAL_TYPE,
    )
    vec = Vector.from_arrow(arr)

    expected = [
        (0, 5_000 * 1_000),
        (0, MICROSECONDS_PER_DAY),
        None,
    ]
    assert vec.to_pylist() == expected

