THIS MODULE ORIGINALLY COPIED FROM: https://github.com/mewwts/pyudorandom
DATE: 2021-12-28

# Pyudorandom
[![build Status](https://travis-ci.org/mewwts/pyudorandom.svg?branch=master)](https://travis-ci.org/mewwts/pyudorandom) [![Coverage Status](https://coveralls.io/repos/mewwts/pyudorandom/badge.svg?branch=master&service=github)](https://coveralls.io/github/mewwts/pyudorandom?branch=master)

The pyudorandom module lets you iterate over a list in a non-succsessive, yet deterministic way. 
It comes in handy when you want to mix up the items, but don't need any guarantees of randomness. Also, it makes sure that it only gives you the elements once.

If you have a iterable of length `n`, pyudorandom will first find a random number `m` between `0` and `n-1` such that the `gcd(m, n) == 1`. That number will then be used to generate `0` through `n-1` by using [integers modulo `n`](http://en.wikipedia.org/wiki/Multiplicative_group_of_integers_modulo_n).

As such, it might be slow on small data, but shall be significantly faster
than random.shuffle for longer lists.

# API
Given a list `ls = [1, 5, 7, 3, ..., 321, 994]` and the imported module `import pyudorandom`.
##pyudorandom.items(ls)
Draw 'random' items from ls.
```Python 
>>> for i in pyudorandom.items(ls):
...     print(i)
...
7
321
...
...
5
```

## pyudorandom.shuffle(ls)
Get a new list with the elements of ls in a new order.
```Python
>>> new_order = pyudorandom.shuffle(ls)
>>> new_order == ls
False
>>> set(new_order) == set(ls)
True
```
## pyudorandom.indices(ls)
Get the indices of the list in a 'random' order.

# Performance 
See source in perf.py.

methods|n=10|n=100|n=1000|n=10000|n=100000|n=1000000|n=10000000|
|:-----|-----------:|------------:|-----------:|----------:|---------:|---------:|---------:|
| random.shuffle | 9.4878e-05 | 0.000697057 | 0.0079944  | 0.0994185 | 1.15775  | 13.1515  | 158.261  |
| pyudorandom.shuffle | 9.2293e-05 | 0.000397103 | 0.00311404 | 0.0421352 | 0.577736 |  6.40441 |  88.2447 |
| pyudorandom.items | 7.2781e-05 | 0.000374695 | 0.00349281 | 0.0377044 | 0.551456 |  6.10636 |  84.6845 |

Pyudorandom can be twice as fast as random.