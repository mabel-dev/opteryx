#!/usr/bin/env python
#coding:utf-8
# Author:  mozman
# Purpose: binary tree module
# Created: 28.04.2010
# Copyright (c) 2010-2013 by Manfred Moitzi
# License: MIT License

"""
The module has been updated from it's original form.

This performs slower than a dict for loading but faster for searching.

High Cardinality (mostly unique)
    Loading - Tree:Dict => approx 6:1
    Searching - Tree:Dict => approx 1:2

Low Cardinality (a lot of duplication)
    Loading - Tree:Dict => approx 1:1
    Searching - Tree:Dict => approx 1:4
"""

from __future__ import absolute_import
from opteryx.imports.accumulation_tree.abctree import ABCTree


__all__ = ['BinaryTree']


class Node(object):
    """Internal object, represents a tree node."""
    __slots__ = ('key', 'value', 'left', 'right')

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.left = None
        self.right = None

    def __getitem__(self, key):
        """N.__getitem__(key) <==> x[key], where key is 0 (left) or 1 (right)."""
        return self.left if key == 0 else self.right

    def __setitem__(self, key, value):
        """N.__setitem__(key, value) <==> x[key]=value, where key is 0 (left) or 1 (right)."""
        if key == 0:
            self.left = value
        else:
            self.right = value


class BinaryTree(ABCTree):
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

    def insert(self, key, value):

        # only index strings at the moment
        if type(key) != str:
            return

        """T.insert(key, value) <==> T[key] = value, insert key, value into tree."""
        if self._root is None:
            self._root = Node(key, [value])
            self._count += 1

        else:
            parent = None
            direction = 0
            node = self._root
            while True:
                if node is None:
                    parent[direction] = Node(key, [value])
                    self._count += 1
                    break
                if key == node.key:
                    node.value.extend([value])
                    break
                else:
                    parent = node
                    direction = 0 if key <= node.key else 1
                    node = node[direction]

    def remove(self, key):
        raise NotImplementedError("cannot remove items")