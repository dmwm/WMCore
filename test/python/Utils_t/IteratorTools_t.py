#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""

from __future__ import division, print_function

from builtins import range
import itertools
import unittest

from Utils.IteratorTools import grouper, flattenList


class IteratorToolsTest(unittest.TestCase):
    """
    unittest for IteratorTools functions
    """

    def testGrouper(self):
        """
        Test the grouper function (returns chunk of an iterable)
        """

        listChunks = [i for i in grouper(list(range(0, 7)), 3)]  # Want list(range) for python 3
        iterChunks = [i for i in grouper(range(0, 7), 3)]  # xrange becomes range in python 3

        for a, b in itertools.zip_longest(listChunks, iterChunks):
            self.assertEqual(a, b)

        self.assertEqual(listChunks[-1], [6])

    def testFlattenList(self):
        """
        Test the flattenList function (returns a flat list out
        of a list of lists)
        """
        doubleList = [list(range(1, 4)), list(range(10, 11)), list(range(15, 18))]
        flatList = flattenList(doubleList)
        self.assertEqual(len(flatList), 7)
        self.assertEqual(set(flatList), set([1, 2, 3, 10, 15, 16, 17]))


if __name__ == '__main__':
    unittest.main()
