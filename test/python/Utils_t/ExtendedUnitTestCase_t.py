#!/usr/bin/env python
"""
Test of assertContentsEqual for ExtendedUnitTestCase
"""

from __future__ import division, print_function

import copy
import unittest

from Utils.ExtendedUnitTestCase import ExtendedUnitTestCase


class ExtendedUnitTestCaseTest(ExtendedUnitTestCase):
    """
    Test of assertContentsEqual for ExtendedUnitTestCase
    """

    def testNestedListComparison(self):
        """
        Test the nested comparison method. Deeply nested lists are compared by content, not order
         (assertItemsEqual in python 2.7, assertCountEqual in python3)
        """

        initialList = [{'a': [1, 2, 3, 4], 'b': ['a', 'b', 'c', 'd'], 'c': 'd'},
                       {'e': {'c': [1, 2, 3]}}]
        copyList = copy.deepcopy(initialList)
        sameList = [{'e': {'c': [3, 2, 1]}},
                    {'a': [3, 1, 2, 4], 'b': ['b', 'a', 'c', 'd'], 'c': 'd'}]
        diffList1 = [{'a': [1, 2, 3, 5], 'b': ['a', 'b', 'c', 'e'], 'c': 'd'}, {'e': {'c': [1, 2, 3]}}]
        diffList2 = [{'a': [1, 2, 3, 4], 'b': ['a', 'b', 'c', 'd'], 'c': 'd'}, {'e': {'c': [3, 1, 3]}}]

        self.assertNotEqual(initialList, sameList)
        self.assertContentsEqual(initialList, sameList)
        self.assertNotEqual(initialList, sameList)  # Make sure they are still not equal (were not modified)
        self.assertEqual(initialList, copyList)

        with self.assertRaises(AssertionError):
            self.assertContentsEqual(initialList, diffList1)
        with self.assertRaises(AssertionError):
            self.assertContentsEqual(initialList, diffList2)

    def testNestedDictComparison(self):
        """
        Test the nested comparison method. Deeply nested lists are compared by content, not order
         (assertItemsEqual in python 2.7, assertCountEqual in python3)
        """

        initialDict = {'a': [1, 2, 3, 4],
                       'b': ['a', 'b', 'c', 'd'],
                       'c': 'd',
                       'e': {'c': [1, 2, 3]}}
        copyDict = copy.deepcopy(initialDict)
        sameDict = {'a': [3, 1, 4, 2],
                    'b': ['a', 'b', 'd', 'c'],
                    'c': 'd',
                    'e': {'c': [3, 2, 1]}}
        diffDict1 = {'a': [3, 1, 4, 2],
                     'b': ['a', 'b', 'd', 'c'],
                     'c': 'd',
                     'e': {'c': 'f'}}
        diffDict2 = {'a': [1, 2, 3, 4],
                     'b': ['a', 'b', 'c', 'd'],
                     'c': 'd',
                     'e': {'c': [3, 2, 3]}}

        self.assertNotEqual(initialDict, sameDict)
        self.assertContentsEqual(initialDict, sameDict)
        self.assertNotEqual(initialDict, sameDict)  # Make sure they are still not equal (were not modified)
        self.assertEqual(initialDict, copyDict)

        with self.assertRaises(AssertionError):
            self.assertContentsEqual(initialDict, diffDict1)
        with self.assertRaises(AssertionError):
            self.assertContentsEqual(initialDict, diffDict2)

    def testWrongTypes(self):
        """
        Test that comparison fails with different types
        """

        d = {}
        l = []
        s = ()

        with self.assertRaises(AssertionError):
            self.assertContentsEqual(d, l)
        with self.assertRaises(AssertionError):
            self.assertContentsEqual(l, s)


if __name__ == '__main__':
    unittest.main()
