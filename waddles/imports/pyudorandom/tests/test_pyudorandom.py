import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import unittest
from pyudorandom import indices, shuffle, items, bin_gcd

class Tests(unittest.TestCase):

    def test_items(self):
        my_list = range(50)
        generated = list(items(my_list))
        self.assertNotEqual(generated, my_list)
        self.assertEqual(set(generated), set(my_list))

    def test_items_empty(self):
        my_list = []
        my_items = items(my_list)
        self.assertFalse(list(my_items))
        self.assertEqual(list(my_items), my_list)

    def test_list_shuffle(self):
        my_list = [1, 2, 4, 5, 7, 8]
        generated = shuffle(my_list)
        self.assertNotEqual(my_list, generated)
        self.assertEqual(set(my_list), set(generated))

    def test_generation_with_odd_number(self):
        n = 45
        elements = set(range(n))
        generated = set([])
        for i in indices(n):
            generated.add(i)
        self.assertEqual(generated, elements)

    def test_generation_with_even_number(self):
        n = 24
        elements = set(range(n))
        generated = set([])
        for i in indices(n):
            generated.add(i)
        self.assertEqual(generated, elements)

    def test_bin_gcd_simple(self):
        self.assertEqual(bin_gcd(4, 3), 1)

    def test_bin_gcd_hard(self):
        self.assertEqual(bin_gcd(54614, 12312), 2)

    def test_bin_gcd_with_zero(self):
        self.assertEqual(bin_gcd(0, 2000), 2000)

if __name__ == '__main__':
    unittest.main()
