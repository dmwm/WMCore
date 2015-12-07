'''
Created on Jun 18, 2009

@author: meloam
'''
import unittest
import os

import WMCore.Storage.StageOutMgr as StageOutMgr

class StageOutMgrTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())

    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
