# type:ignore
# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))
import pytest
from pytest import approx
import distogram


def test_update():
    h = distogram.Distogram(bin_count=3)

    # fill histogram
    distogram.update(h, 23)
    assert h.bins == [(23, 1)]
    distogram.update(h, 28)
    assert h.bins == [(23, 1), (28, 1)]
    distogram.update(h, 16)
    assert h.bins == [(16, 1), (23, 1), (28, 1)]

    # update count on existing value
    distogram.update(h, 23)
    assert h.bins == [(16, 1), (23, 2), (28, 1)]
    distogram.update(h, 28)
    assert h.bins == [(16, 1), (23, 2), (28, 2)]
    distogram.update(h, 16)
    assert h.bins == [(16, 2), (23, 2), (28, 2)]

    # merge values
    h = distogram.update(h, 26)
    assert h.bins[0] == (16, 2)
    assert h.bins[1] == (23, 2)
    assert h.bins[2][0] == approx(27.33333)
    assert h.bins[2][1] == 3


def test_update_with_invalid_count():
    h = distogram.Distogram(bin_count=3)

    with pytest.raises(ValueError):
        distogram.update(h, 23, count=0)
