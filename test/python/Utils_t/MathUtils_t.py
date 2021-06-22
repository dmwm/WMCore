#!/usr/bin/env python
"""
Unittests for MathUtils functions
"""

from __future__ import division, print_function

import unittest

from Utils.MathUtils import quantize


class MathUtilsTest(unittest.TestCase):
    """
    unittest for MathUtils functions
    """

    def testQuantize(self):
        """
        Test the quantize function
        """
        self.assertEqual(quantize(15, 5), 15)
        self.assertEqual(quantize(14, 5), 15)
        self.assertEqual(quantize(16, 5), 20)

        self.assertEqual(quantize(15, 5.0), 15)
        self.assertEqual(quantize(14, 5.0), 15)
        self.assertEqual(quantize(16, 5.0), 20)

        self.assertRaises(ValueError, quantize, [1], 50)
        self.assertRaises(ValueError, quantize, {1}, 50)

        self.assertRaises(ValueError, quantize, 1, [50])
        self.assertRaises(ValueError, quantize, 1, {50})


if __name__ == "__main__":
    unittest.main()