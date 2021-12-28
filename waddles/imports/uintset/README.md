THIS MODULE ORIGINALLY COPIED FROM: https://github.com/standupdev/uintset/
DATE: 2021-12-28

# uintset

A Python set type designed for dense sets of non-negative integers. Each element is represented as a `1` bit in the corresponding position within a Python `int`.

Membership test `n in s` is _O(1)_ because the bit that represents `n` in the `s` set is in word `n // 64`, at bit offset `n % 64` in the internal representation of the `int`.

Adding an element means setting a bit at the corresponding offset.

Set operations such as union, intersection, and symmetric difference are implemented using the bitwise operators `|`, `&`, and `^` on the `int` values that store the bits.

> This package is inspired by the `intset` example in chapter 6 of
_The Go Programming Language_, by Donovan & Kernighan.