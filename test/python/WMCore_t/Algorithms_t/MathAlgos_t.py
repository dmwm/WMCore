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

if __name__ == "__main__":
    unittest.main() 
