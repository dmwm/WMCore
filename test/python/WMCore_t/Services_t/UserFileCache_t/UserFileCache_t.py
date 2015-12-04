#!/usr/bin/env python
"""
Test case for UserFileCache
"""

import unittest
import filecmp
import os
from os import path

from WMCore.Services.UserFileCache.UserFileCache import UserFileCache, calculateChecksum
from WMCore.WMBase import getTestBase

class UserFileCacheTest(unittest.TestCase):
    """
    Unit tests for UserFileCache Service
    """


    def testChecksum(self):
        """
        Tests checksum method
        """
        checksum1 = calculateChecksum(tarfile_=path.join(getTestBase(), 'WMCore_t/Services_t/UserFileCache_t/ewv_crab_EwvAnalysis_31_111229_140959_publish.tgz'))
        checksum2 = calculateChecksum(tarfile_=path.join(getTestBase(), 'WMCore_t/Services_t/UserFileCache_t/ewv_crab_EwvAnalysis_31_resubmit_111229_144319_publish.tgz'))
        self.assertTrue(checksum1)
        self.assertTrue(checksum2)
        self.assertFalse(checksum1 == checksum2)

        self.assertRaises(IOError, calculateChecksum, **{'tarfile_': 'does_not_exist'})
        return


    def testUploadDownload(self):
        if 'UFCURL' in os.environ:
            currdir = getTestBase()
            upfile = path.join(currdir, 'WMCore_t/Services_t/UserFileCache_t/test_file.tgz') #file to upload
            upfileLog = path.join(currdir, 'WMCore_t/Services_t/UserFileCache_t/uplog.txt') #file to upload
            ufc = UserFileCache({'endpoint':os.environ['UFCURL'], 'pycurl': True})

            #hashkey upload/download
            res = ufc.upload(upfile)
            ufc.download(res['hashkey'], output='pippo_publish_down.tgz')

            #hashkey deletion
            ufc.removeFile(res['hashkey'])

            #log upload/download
            res = ufc.uploadLog(upfileLog)
            ufc.downloadLog(upfileLog, upfileLog+'.downloaded')
            self.assertTrue(filecmp.cmp(upfileLog, upfileLog+'.downloaded'))



if __name__ == '__main__':
    unittest.main()
