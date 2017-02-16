#!/usr/bin/env python
"""
Unittests for WMWorkloadTools functions
"""

from __future__ import division, print_function

import unittest

from WMCore.WMSpec.WMWorkloadTools import makeList, makeNonEmptyList
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException


class WMWorkloadToolsTest(unittest.TestCase):
    """
    unittest for WMWorkloadTools functions
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

        self.assertRaises(WMSpecFactoryException, makeList, 123)
        self.assertRaises(WMSpecFactoryException, makeList, 123.456)
        self.assertRaises(WMSpecFactoryException, makeList, {1: 123})

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

        self.assertRaises(WMSpecFactoryException, makeNonEmptyList, [])
        self.assertRaises(WMSpecFactoryException, makeNonEmptyList, "")


if __name__ == '__main__':
    unittest.main()
