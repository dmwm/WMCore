#!/usr/bin/env python
"""
Unittests for PSetTweaks module

"""

from __future__ import division, print_function

import itertools
import unittest

from Utils.IterTools import grouper


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

        self.assertEqual(listChunks[-1], [6])


if __name__ == '__main__':
    unittest.main()
