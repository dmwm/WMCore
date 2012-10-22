#!/usr/bin/env python
"""
_MathAlgos_t_

Test class for basic Math Algorithms
"""


import unittest

from WMCore.Algorithms import MathAlgos

class MathAlgoTest(unittest.TestCase):
    """
    Tests for basic Math Algorithms

    """


    def setUp(self):
        """
        Do nothing

        """
        return

    def tearDown(self):
        """
        Do nothing

        """
        return

    def testAverageStdDev(self):
        """
        _testAverageStdDev_

        Test average, standard deviation function
        """

        numList = ['a', 'b', 'c']
        self.assertRaises(MathAlgos.MathAlgoException,
                          MathAlgos.getAverageStdDev, numList)

        numList = [1, 1, 1, 1, 1, 1, 1, 1]
        result = MathAlgos.getAverageStdDev(numList = numList)
        self.assertEqual(result[0], 1.0)  # Average should be zero
        self.assertEqual(result[1], 0.0)  # stdDev should be zero

        numList = [1, 2, 3, 4, 5, 6, 7, 8]
        result = MathAlgos.getAverageStdDev(numList = numList)
        self.assertEqual(result[0], 4.5)
        self.assertEqual(result[1], 2.2912878474779199) # I think this is right

        return

    def testTruncate(self):
        """
        _testTruncate_

        Test the floorTruncate function
        """

        self.assertEqual(MathAlgos.floorTruncate(1.23456), 1.234)
        return


    def testHistogram(self):
        """
        _testHistogram_

        Test our ability to build histogram objects out of lists
        """

        # Check that we override correctly when we give a uniform list
        numList = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        result = MathAlgos.createHistogram(numList = numList, nBins = 10, limit = 10)
        self.assertEqual(result[0]['lowerEdge'], 0.0)
        self.assertEqual(result[0]['upperEdge'], 2.0)
        self.assertEqual(result[0]['nEvents'],   10)

        numList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        result = MathAlgos.createHistogram(numList = numList, nBins = 2, limit = 10)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['nEvents'], 5)
        self.assertEqual(result[1]['nEvents'], 5)
        self.assertEqual(result[0]['average'], 3.0)
        self.assertEqual(result[1]['average'], 8.0)
        self.assertEqual(result[0]['stdDev'],  1.4142135623730951)
        self.assertEqual(result[1]['stdDev'],  1.4142135623730951)

        # Check that we generate overflow and underflow bins correctly
        result = MathAlgos.createHistogram(numList = numList, nBins = 2, limit = 1)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]['type'], 'underflow')
        self.assertEqual(result[1]['type'], 'overflow')
        self.assertEqual(result[0]['nEvents'], 2)
        self.assertEqual(result[1]['nEvents'], 2)
        self.assertEqual(result[0]['stdDev'], 0.5)
        self.assertEqual(result[1]['stdDev'], 0.5)


    def testSortListByKeys(self):
        """
        _testSortListByKeys_

        Test our ability to sort a list of dictionaries in the order of a single
        numerical key
        """

        l = [{'a': 102, 'b': 200, 'name': 'One'},
             {'a': 101, 'b': 199, 'name': 'Two'},
             {'a': 100, 'b': 198, 'name': 'Three'},
             {'a': 103, 'b': 197, 'name': 'Four'}]

        result = MathAlgos.sortDictionaryListByKey(dictList = l, key = 'a')
        self.assertEqual(result[0]['name'], 'Three')
        self.assertEqual(result[1]['name'], 'Two')
        self.assertEqual(result[2]['name'], 'One')
        self.assertEqual(result[3]['name'], 'Four')

        result = MathAlgos.sortDictionaryListByKey(dictList = l, key = 'b')
        self.assertEqual(result[0]['name'], 'Four')
        self.assertEqual(result[1]['name'], 'Three')
        self.assertEqual(result[2]['name'], 'Two')
        self.assertEqual(result[3]['name'], 'One')

        result = MathAlgos.sortDictionaryListByKey(dictList = l, key = 'b',
                                                   reverse = True)
        self.assertEqual(result[3]['name'], 'Four')
        self.assertEqual(result[2]['name'], 'Three')
        self.assertEqual(result[1]['name'], 'Two')
        self.assertEqual(result[0]['name'], 'One')


        # This shouldn't fail, it just should return a flat list
        result = MathAlgos.sortDictionaryListByKey(dictList = l, key = 'c',
                                                   reverse = True)


        return

    def testGetLargestValue(self):
        """
        _testGetLargestValue_

        See if we can get the largest value from a list of histograms
        for a specific key
        """

        l = [{'a': 102, 'b': 200, 'name': 'One'},
             {'a': 101, 'b': 199, 'name': 'Two'},
             {'a': 100, 'b': 198, 'name': 'Three'},
             {'a': 103, 'b': 197, 'name': 'Four'}]

        result = MathAlgos.getLargestValues(dictList = l, key = 'a', n = 2)
        self.assertEqual(result, [{'a': 103, 'b': 197, 'name': 'Four'},
                                  {'a': 102, 'b': 200, 'name': 'One'}])
        result = MathAlgos.getLargestValues(dictList = l, key = 'b', n = 3)
        self.assertEqual(result, [{'a': 102, 'b': 200, 'name': 'One'},
                                  {'a': 101, 'b': 199, 'name': 'Two'},
                                  {'a': 100, 'b': 198, 'name': 'Three'}])
        return


if __name__ == "__main__":
    unittest.main()
