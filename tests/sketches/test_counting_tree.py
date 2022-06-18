import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.sketches.counting_tree import CountingTree

def test_counter():
    pass


if __name__ == "__main__":
    ct = CountingTree()
    for i in range(20):
        for u in range(i):
            #ct.insert(i)
            ct.insert(u)

    print(ct)