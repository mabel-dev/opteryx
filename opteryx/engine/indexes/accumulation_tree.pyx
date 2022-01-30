# cython: language_level=3
#!/usr/bin/env python
#coding:utf-8
# Author:  mozman
# Purpose: binary tree module
# Created: 28.04.2010
# Copyright (c) 2010-2013 by Manfred Moitzi
# License: MIT License

"""
The module has been updated from it's original form to improve performance.

This outperforms python dictionaries for data with a lot of duplication.
"""
from typing import List
from opteryx.third_party.accumulation_tree.abctree import ABCTree

__all__ = ['AccumulationTree']


cdef class Node:
    """Internal object, represents a tree node."""
    __slots__ = {'key', 'value', 'left', 'right'}

    cdef readonly object key
    cdef readonly object value
    cdef readonly Node left
    cdef readonly Node right

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.left = None
        self.right = None

    cdef Node get(self, int key):
        return self.left if key == 0 else self.right

    cdef void set(self, int key, Node value):
        if key == 0:
            self.left = value
        else:
            self.right = value

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def _value(self, value):
        self.value = value

cdef class _AccumulationTree(object):
    """
    BinaryTree implements an unbalanced binary tree with a dict-like interface.

    see: http://en.wikipedia.org/wiki/Binary_tree

    A binary tree is a tree data structure in which each node has at most two
    children.

    BinaryTree() -> new empty tree.
    BinaryTree(mapping,) -> new tree initialized from a mapping
    BinaryTree(seq) -> new tree initialized from seq [(k1, v1), (k2, v2), ... (kn, vn)]

    see also abctree.ABCTree() class.
    """
    __slots__ = ["_root"]

    cdef public Node _root

    def insert(self, key, value):
        if self._root is None:
            self._root = Node(key, value)
            return

        cdef Node parent = None
        cdef int direction = 0

        node = self._root
        while 1:
            if node is None:
                parent[direction] = Node(key, value)
                break
            if key == node.key:
                node._value(value)
                break
            else:
                parent = node
                direction = 0 if key <= node.key else 1
                node = node[direction]

    def remove(self, key):
        raise NotImplementedError("AccumulationTree is additive only, you cannot remove items.")


class AccumulationTree(_AccumulationTree, ABCTree):
    pass
