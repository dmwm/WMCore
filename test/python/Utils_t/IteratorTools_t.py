#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""

from builtins import range
import itertools
import unittest

from Utils.IteratorTools import grouper, flattenList, makeListElementsUnique, getChunk


class IteratorToolsTest(unittest.TestCase):
    """
    unittest for IteratorTools functions
    """

    def testGrouper(self):
        """
        Test the grouper function (returns chunk of an iterable)
        """

        listChunks = [i for i in grouper(list(range(0, 7)), 3)]  # Want list(range) for python 3
        iterChunks = [i for i in grouper(list(range(0, 7)), 3)]  # xrange becomes range in python 3

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

    def testNoDupListOfLists(self):
        """
        Test the `makeListElementsUnique` function (which returns a list with
        unique elements)
        """
        expRes = [[1], [2], [3], [4]]
        inputTest = [[1], [2], [3], [4]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        inputTest = [[1], [2], [3], [4], [1]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        inputTest = [[1], [2], [3], [4], [1], [1], [2], [3]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        # trying a different data type
        expRes = [[2, 20], [4, 40], [4, 41], [5, 40]]
        inputTest = [[2, 20], [4, 40], [4, 41], [5, 40], [4, 40], [4, 41]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        inputTest = [[5, 40], [2, 20], [4, 40], [4, 41], [5, 40], [4, 40], [4, 41]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        # and now with strings
        expRes = [["2", "20"], ["4", "40"], ["4", "41"], ["5", "40"]]
        inputTest = [["2", "20"], ["4", "40"], ["4", "41"], ["5", "40"], ["4", "40"], ["4", "41"]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        inputTest = [["5", "40"], ["2", "20"], ["4", "40"], ["4", "41"], ["5", "40"], ["4", "40"], ["4", "41"]]
        self.assertListEqual(expRes, makeListElementsUnique(inputTest))

        # now test with a list of tuples
        data = [(1, 2), (1, 3), (2, 4), (2, 5), (1, 3), (2, 5)]
        self.assertListEqual([(1, 2), (1, 3), (2, 4), (2, 5)], makeListElementsUnique(data))

    def testGetChunk(self):
        """
        Test the `getChunk` function.
        """
        arr = range(10)
        for chunk in getChunk(arr, 2):
            self.assertTrue(len(chunk) == 2)

if __name__ == '__main__':
    unittest.main()
