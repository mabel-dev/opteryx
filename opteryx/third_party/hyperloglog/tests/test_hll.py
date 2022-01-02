import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pytest

from hyperloglog import HyperLogLog, get_alpha
from hyperloglog import biasData, tresholdData, rawEstimateData
from hyperloglog import *
import math
import os
import pickle



def test_blobs():
    assert len(tresholdData) == 18 - 3

def test_alpha():
    alpha = [get_alpha(b) for b in range(4, 10)]
    assert alpha == [0.673, 0.697, 0.709, 0.7152704932638152, 0.7182725932495458, 0.7197831133217303]

def test_alpha_bad():

    with pytest.raises(ValueError):
        get_alpha(1)

    with pytest.raises(ValueError):
        get_alpha(17)


def test_init():
    s = HyperLogLog(0.05)
    assert s.p == 9
    assert s.alpha == 0.7197831133217303
    assert s.m == 512
    assert len(s.M) == 512

def test_add():

    # We need a deterministic hash algo for testing, the default is to use hash()
    # which is fast but not deterministic
    from cityhash import CityHash32

    s = HyperLogLog(0.05)

    for i in range(10):
        s.add(str(i), hash_func=CityHash32)

    M = [(i, v) for i, v in enumerate(s.M) if v > 0]

    assert M == [(78, 33), (107, 33), (205, 34), (237, 35), (290, 33), (327, 34), (363, 33), (415, 33), (429, 33), (439, 33)]

def test_calc_cardinality():

    clist = [1, 5, 10, 100, 1000, 5000, 10000, 50000, 100000]
    n = 5
    rel_err = 0.005

    from cityhash import CityHash64

    group = []

    for card in clist:
        group = []
        # do this 25 times so we get an approximation
        for c in range(n):
            print(f"cycle {c} for {card}")
            a = HyperLogLog(rel_err)

            # add random values to the log
            for i in range(card):
                a.add(os.urandom(64))

            group.append(a.card())
            print(a.card())

        # we average out the results to reduce outliers
        res = sum(group) / len(group)

        # we  should be within 1% of the actual answer
        assert (res * 1.01) > card > (res * 0.99), f"{res} not within 1% of {card}"

def test_update():
    a = HyperLogLog(0.05)
    b = HyperLogLog(0.05)
    c = HyperLogLog(0.05)

    for i in range(2):
        a.add(str(i))
        c.add(str(i))

    for i in range(2, 4):
        b.add(str(i))
        c.add(str(i))

    a.update(b)

    assert a != b
    assert b != c
    assert a == c


def test_update_err():
    a = HyperLogLog(0.05)
    b = HyperLogLog(0.01)

    with pytest.raises(ValueError):
        a.update (b)
