#!/usr/bin/env python
"""
Unittests for Utilities functions
"""

from __future__ import division, print_function

import unittest
from Utils.Utilities import makeList, makeNonEmptyList, strToBool, safeStr, rootUrlJoin, zipEncodeStr
from Utils.Utilities import lowerCmsHeaders


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

    def testLowerCmsHeaders(self):
        "Test lowerCmsHeaders function"
        val = 'cms-xx-yy'
        headers = {'CAPITAL':1, 'Camel':1, 'Cms-Xx-Yy': val, 'CMS-XX-YY': val, 'cms-xx-yy': val}
        lheaders = lowerCmsHeaders(headers)
        self.assertEqual(sorted(lheaders.keys()), sorted(['CAPITAL', 'Camel', val]))
        self.assertEqual(lheaders['CAPITAL'], 1)
        self.assertEqual(lheaders['Camel'], 1)
        self.assertEqual(lheaders[val], val)
        self.assertEqual(len(lheaders.keys()), 3)

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

    def testZipEncodeStr(self):
        """
        Test the zipEncodeStr function.
        """
        message = """
%MSG-s CMSException:  AfterFile 02-Jun-2010 14:31:43 CEST PostEndRun
cms::Exception caught in cmsRun
---- EventProcessorFailure BEGIN
EventProcessingStopped
---- ScheduleExecutionFailure BEGIN
ProcessingStopped
---- InvalidReference BEGIN
BadRefCore Attempt to dereference a RefCore containing an invalid
ProductID has been detected. Please modify the calling
code to test validity before dereferencing.
cms::Exception going through module PatMCMatching/analyzePatMCMatching run: 1 lumi: 666672 event: 305
---- InvalidReference END
Exception going through path p
---- ScheduleExecutionFailure END
an exception occurred during current event processing
cms::Exception caught in CMS.EventProcessor and rethrown
---- EventProcessorFailure END
"""
        encodedMessage = \
            'eNp1j8FqwzAMhu95Cl0G2yEhaXvyrU3dkkFHqfcCnq02hkQOtlz6+HM2MrbDdBLS9/1CxdNJHcsI7UnJh8GJnScBsL0yhoMbEOpV+ZqoXNVNDc1GrBuxWUMr1TucfWRJ9pKoMGMU4scHo9OtZ3C5G+O8L3OBvCPxOXiDMfpw0G5IAWEnj91b8Xvn6KbYTxPab0+ZHm0aUD7QpDn/r/qP1dFdD85e8IoBySz0Ts+j1md9y4zjxMAebGYWTsMCGE+sHeVk0JS/+Qqc79lkuNtDryN8IBLAc1VVL5+o0W8i'
        self.assertEqual(zipEncodeStr(message, maxLen=300, compressLevel=9, steps=10, truncateIndicator=" (...)"), encodedMessage)
        # Test different maximum lengths
        # Encoded message should always be less than the maximum limit.
        for maxLen in (800, 500, 20):
            self.assertLessEqual(len(zipEncodeStr(message, maxLen=maxLen, compressLevel=9, steps=10, truncateIndicator=" (...)")), maxLen)

if __name__ == '__main__':
    unittest.main()
