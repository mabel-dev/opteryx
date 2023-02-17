import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils.btree import BTree


def test_insert_and_search():
    btree = BTree(3)
    btree.insert(1)
    btree.insert(2)
    btree.insert(3)
    btree.insert(4)
    btree.insert(5)
    assert btree.search(btree.root, 3)
    assert not btree.search(btree.root, 6)


def test_save_and_load():
    btree = BTree(3)
    btree.insert(1)
    btree.insert(2)
    btree.insert(3)
    btree.insert(4)
    btree.insert(5)

    filename = str(".temp/btree.txt")
    btree.save_to_storage(filename)

    btree2 = BTree(3)
    btree2.load_from_storage(filename)

    result1 = btree.traverse(btree.root)
    result2 = btree2.traverse(btree2.root)
    assert result1 == result2


def test_empty_tree():
    btree = BTree(3)
    result = btree.traverse(btree.root)
    assert result == []


def test_split_child():
    btree = BTree(3)
    btree.root.keys = [3, 7, 11]
    btree.root.children = [
        BTree.Node(keys=[1, 2]),
        BTree.Node(keys=[4, 5]),
        BTree.Node(keys=[8, 9]),
        BTree.Node(keys=[12, 13]),
    ]
    btree.split_child(btree.root, 1)
    assert btree.root.keys == [2, 3, 7, 11]
    assert [node.keys for node in btree.root.children] == [
        [1],
        [4, 5],
        [8, 9],
        [12, 13],
    ]


if __name__ == "__main__":  # pragma: no cover
    test_empty_tree()
    test_insert_and_search()
    test_save_and_load()
    test_split_child()

    print("✅ okay")
