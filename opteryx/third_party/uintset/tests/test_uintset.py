import pytest

import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from uintset import UintSet

WORD_SIZE = 64


def test_new():
    s = UintSet()
    assert len(s) == 0


def test_new_from_iterable():
    s = UintSet([1, 100, 3])  # beyond word 0
    assert len(s) == 3


def test_add():
    s = UintSet()
    s.add(0)
    assert len(s) == 1


def test_add_multiple():
    s = UintSet()
    s.add(1)
    s.add(3)
    s.add(1)
    assert len(s) == 2


def test_add_negative():
    s = UintSet()
    with pytest.raises(ValueError):
        s.add(-1)


def test_contains():
    s = UintSet()
    s.add(1)
    assert 1 in s


def test_iter():
    s = UintSet([1, 5, 0, 3, 2, 4])
    assert list(s) == [0, 1, 2, 3, 4, 5]


def test_repr_empty():
    s = UintSet()
    assert repr(s) == "UintSet()"


def test_repr():
    s = UintSet([1, 5, 0, 3, 2, 4])
    assert repr(s) == "UintSet({0, 1, 2, 3, 4, 5})"


def test_eq():
    test_cases = [
        (UintSet(), UintSet(), True),
        (UintSet([1]), UintSet(), False),
        (UintSet(), UintSet([1]), False),
        (UintSet([1, 2, 100]), UintSet([100, 2, 1]), True),  # beyond word 0
        (UintSet([1, 100]), UintSet([1, 101]), False),
        (UintSet([1, 100]), UintSet([1, 100, 1000]), False),
    ]
    for s1, s2, want in test_cases:
        assert (s1 == s2) is want


def test_copy():
    test_cases = [
        UintSet(),
        UintSet([1]),
        UintSet([1, 2]),
        UintSet([1, 100]),  # beyond word 0
    ]
    for s1 in test_cases:
        s2 = s1.copy()
        assert s1 == s2


union_cases = [
    (UintSet(), UintSet(), UintSet()),
    (UintSet([1]), UintSet(), UintSet([1])),
    (UintSet(), UintSet([1]), UintSet([1])),
    (UintSet([1, 100]), UintSet([100, 1]), UintSet([100, 1])),  # beyond word 0
    (UintSet([1, 100]), UintSet([2]), UintSet([1, 2, 100])),
]


@pytest.mark.parametrize("s1, s2, want", union_cases)
def test_or_op(s1, s2, want):
    got = s1 | s2
    assert len(got) == len(want)
    assert got == want


@pytest.mark.parametrize("s1, s2, want", union_cases)
def test_union(s1, s2, want):
    got = s1.union(s2)
    assert len(got) == len(want)
    assert got == want


@pytest.mark.parametrize("s1, s2, want", union_cases)
def test_union_iterable(s1, s2, want):
    it = list(s2)
    got = s1.union(it)
    assert len(got) == len(want)
    assert got == want


def test_union_iterable_multiple():
    s = UintSet([1, 3, 5])
    it1 = [2, 4, 6]
    it2 = {10, 11, 12}
    want = UintSet({1, 2, 3, 4, 5, 6, 10, 11, 12})
    got = s.union(it1, it2)
    assert got == want


@pytest.fixture
def intersection_cases():
    return [
        (UintSet(), UintSet(), UintSet()),
        (UintSet([1]), UintSet(), UintSet()),
        (UintSet([1]), UintSet([1]), UintSet([1])),
        (UintSet([1, 100]), UintSet([100, 1]), UintSet([100, 1])),  # beyond word 0
        (UintSet([1, 100]), UintSet([2]), UintSet()),
        (UintSet([1, 2, 3, 4]), UintSet([2, 3, 5]), UintSet([2, 3])),
    ]


def test_and_op(intersection_cases):
    for s1, s2, want in intersection_cases:
        got = s1 & s2
        assert len(got) == len(want)
        assert got == want


def test_intersection(intersection_cases):
    for s1, s2, want in intersection_cases:
        got = s1.intersection(s2)
        assert len(got) == len(want)
        assert got == want


@pytest.fixture
def symmetric_diff_cases():
    return [
        (UintSet(), UintSet(), UintSet()),
        (UintSet([1]), UintSet(), UintSet([1])),
        (UintSet([1]), UintSet([1]), UintSet()),
        (UintSet([1, 100]), UintSet([100, 1]), UintSet()),  # beyond word 0
        (UintSet([1, 100]), UintSet([2]), UintSet([1, 100, 2])),
        (UintSet([1, 2, 3, 4]), UintSet([2, 3, 5]), UintSet([1, 4, 5])),
    ]


def test_xor_op(symmetric_diff_cases):
    for s1, s2, want in symmetric_diff_cases:
        got = s1 ^ s2
        assert len(got) == len(want)
        assert got == want


def test_symmetric_difference(symmetric_diff_cases):
    for s1, s2, want in symmetric_diff_cases:
        got = s1.symmetric_difference(s2)
        assert len(got) == len(want)
        assert got == want


difference_cases = [
    (UintSet(), UintSet(), UintSet()),
    (UintSet([1]), UintSet(), UintSet([1])),
    (UintSet([1]), UintSet([1]), UintSet()),
    (UintSet([1, 100]), UintSet([100, 1]), UintSet()),  # beyond word 0
    (UintSet([1, 100]), UintSet([2]), UintSet([1, 100])),
    (UintSet([1, 2, 3, 4]), UintSet([2, 3, 5]), UintSet([1, 4])),
]


@pytest.mark.parametrize("s1, s2, want", difference_cases)
def test_sub_op(s1, s2, want):
    got = s1 - s2
    assert len(got) == len(want)
    assert got == want


@pytest.mark.parametrize("s1, s2, want", difference_cases)
def test_difference(s1, s2, want):
    got = s1.difference(s2)
    assert len(got) == len(want)
    assert got == want


def test_remove():
    test_cases = [
        (UintSet([0]), 0, UintSet()),
        (UintSet([1, 2, 3]), 2, UintSet([1, 3])),
    ]
    for s, elem, want in test_cases:
        s.remove(elem)
        assert s == want


def test_remove_all():
    elems = [1, 2, 3]
    set = UintSet(elems)
    for e in elems:
        set.remove(e)
    assert len(set) == 0


def test_remove_not_found():
    s = UintSet()
    elem = 1
    with pytest.raises(KeyError) as excinfo:
        s.remove(elem)
    assert str(excinfo.value) == str(elem)


def test_remove_not_found_2():
    s = UintSet([1, 3])
    elem = 2
    with pytest.raises(KeyError) as excinfo:
        s.remove(elem)
    assert str(excinfo.value) == str(elem)


def test_pop_not_found():
    s = UintSet()
    with pytest.raises(KeyError) as excinfo:
        s.pop()
    assert "pop from an empty set" in str(excinfo.value)


def test_pop():
    test_cases = [0, 1, WORD_SIZE - 1, WORD_SIZE, WORD_SIZE + 1, 100]
    for want in test_cases:
        s = UintSet([want])
        got = s.pop()
        assert got == want
        assert len(s) == 0


def test_pop_all():
    want = [0, 1, 100]
    s = UintSet(want)
    got = []
    while s:
        got.append(s.pop())
        assert len(s) == (len(want) - len(got))
    assert got == want
