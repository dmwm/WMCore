#!/usr/bin/env python
"""
Unittests for Utilities functions
"""

from __future__ import division, print_function

import unittest
from Utils.Utilities import makeList, makeNonEmptyList, strToBool, safeStr, rootUrlJoin


class UtilitiesTests(unittest.TestCase):
    """
    unittest for Utilities functions
    """

    def testMakeList(self):
        """
        Test the makeList function
        """
        self.assertEqual(makeList([]), [])
        self.assertEqual(makeList(""), [])
        self.assertEqual(makeList(['123']), ['123'])
        self.assertEqual(makeList([456]), [456])
        self.assertItemsEqual(makeList(['123', 456, '789']), ['123', 456, '789'])

        self.assertEqual(makeList('123'), ['123'])
        self.assertEqual(makeList(u'123'), [u'123'])
        self.assertItemsEqual(makeList('123,456'), ['123', '456'])
        self.assertItemsEqual(makeList(u'123,456'), [u'123', u'456'])
        self.assertItemsEqual(makeList('["aa","bb","cc"]'), ['aa', 'bb', 'cc'])
        self.assertItemsEqual(makeList(u' ["aa", "bb", "cc"] '), ['aa', 'bb', 'cc'])

        self.assertRaises(ValueError, makeList, 123)
        self.assertRaises(ValueError, makeList, 123.456)
        self.assertRaises(ValueError, makeList, {1: 123})

    def testMakeNonEmptyList(self):
        """
        Test the makeNonEmptyList function.
        It has exactly the same behaviour as makeList, but throws an exception
        for empty list or string.
        """
        self.assertEqual(makeList(['123']), makeNonEmptyList(['123']))
        self.assertItemsEqual(makeList(['123', 456, '789']), makeNonEmptyList(['123', 456, '789']))

        self.assertItemsEqual(makeList(u'123,456'), makeNonEmptyList(u'123, 456'))
        self.assertItemsEqual(makeList('["aa","bb","cc"]'), makeNonEmptyList('["aa", "bb", "cc"]'))

        self.assertRaises(ValueError, makeNonEmptyList, [])
        self.assertRaises(ValueError, makeNonEmptyList, "")

    def testStrToBool(self):
        """
        Test the strToBool function.
        """
        for v in [True, "True", "TRUE", "true"]:
            self.assertTrue(strToBool(v))
        for v in [False, "False", "FALSE", "false"]:
            self.assertFalse(strToBool(v))

        for v in ["", "alan", [], [''], {'a': 123}]:
            self.assertRaises(ValueError, strToBool, v)

    def testSafeStr(self):
        """
        Test the safeStr function.
        """
        for v in ['123', u'123', 123]:
            self.assertEqual(safeStr(v), '123')
        self.assertEqual(safeStr(123.45), '123.45')
        self.assertEqual(safeStr(False), 'False')
        self.assertEqual(safeStr(None), 'None')
        self.assertEqual(safeStr(""), "")

        for v in [[1, 2], {'x': 123}, set([1])]:
            self.assertRaises(ValueError, safeStr, v)

    def testRootUrlJoin(self):
        """
        Test the testRootUrlJoin function.
        """
        urlbase = "root://site.domain//somepath"
        self.assertEqual(rootUrlJoin(urlbase, "extend"), "root://site.domain//somepath/extend")
        urlbase = "random"
        self.assertEqual(rootUrlJoin(urlbase, "extend"), None)
        urlbase = None
        self.assertEqual(rootUrlJoin(urlbase, "extend"), None)

if __name__ == '__main__':
    unittest.main()
