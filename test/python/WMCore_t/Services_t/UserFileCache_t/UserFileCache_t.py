#!/usr/bin/env python
"""
Test case for UserFileCache
"""

import unittest
from os import path

from WMCore.Services.UserFileCache.UserFileCache import UserFileCache
from WMCore.WMBase import getTestBase

class UserFileCacheTest(unittest.TestCase):
    """
    Unit tests for UserFileCache Service
    """

    def setUp(self):
        """
        Setup for unit tests
        """
        self.ufc = UserFileCache()

    def testChecksum(self):
        """
        Tests checksum method
        """
        checksum1 = self.ufc.checksum(fileName=path.join(getTestBase(), '../data/ewv_crab_EwvAnalysis_31_111229_140959_publish.tgz'))
        checksum2 = self.ufc.checksum(fileName=path.join(getTestBase(), '../data/ewv_crab_EwvAnalysis_31_resubmit_111229_144319_publish.tgz'))
        self.assertTrue(checksum1)
        self.assertTrue(checksum2)
        self.assertFalse(checksum1 == checksum2)

        self.assertRaises(IOError, self.ufc.checksum, **{'fileName': 'does_not_exist'})
        return


if __name__ == '__main__':
    unittest.main()
