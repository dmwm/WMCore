'''
Created on Nov. 7, 2023 by Duong Nguyen
'''
import unittest
import os

from WMCore.WMBase import getTestBase

from WMCore.Storage.StageInMgr import StageInMgr
from WMCore.Storage.SiteLocalConfig import SiteLocalConfig, SiteConfigError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig


class StageInMgrTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())
        os.putenv('SITECONFIG_PATH', os.getcwd())

    def testStageInMgr(self):
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL'
        stageInMgr = StageInMgr()
        stageInMgr.bypassImpl = True
         #keep using 'phedex-node' for overrideParams (to be compatible with the whole DMWM structure?)
        stageInMgr_override = StageInMgr(**{"command":"gfal2","phedex-node":"T1_US_FNAL_Disk","lfn-prefix":"root://abc/xyz"})
        stageInMgr_override.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageInMgr(**fileToStage)
        stageInMgr_override(**fileToStage)

        return


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
