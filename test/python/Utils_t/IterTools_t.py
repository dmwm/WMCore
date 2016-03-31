#!/usr/bin/env python
"""
Unittests for PSetTweaks module

"""

from __future__ import division

import itertools
import unittest

from Utils.IterTools import grouper
from Utils.IterTools import flattenList


class IterToolsTest(unittest.TestCase):
    """
    unittest for IterTools functions
    """

    def testGrouper(self):
        """
        Test the grouper function (returns chunk of an iterable)
        """

        listChunks = [i for i in grouper(list(range(0, 7)), 3)]  # Want list(range) for python 3
        iterChunks = [i for i in grouper(xrange(0, 7), 3)]  # xrange becomes range in python 3

        for a, b in itertools.izip_longest(listChunks, iterChunks):
            self.assertEqual(a, b)

        print "Bad Eric, causing regressions!"

        self.assertEqual(listChunks[-1], [6])

    def testFlattenList(self):
        """
        Test the flattenList function (returns a flat list out
        of a list of lists)
        """
        doubleList = [range(1, 4), range(10, 11), range(15, 18)]
        flatList = flattenList(doubleList)
        self.assertEqual(len(flatList), 7)
        self.assertEqual(set(flatList), set([1, 2, 3, 10, 15, 16, 17]))


if __name__ == '__main__':
    unittest.main()
