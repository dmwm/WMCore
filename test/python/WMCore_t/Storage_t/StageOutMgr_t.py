'''
Created on Jun 18, 2009

@author: meloam

Modified on Nov. 7, 2023 by Duong Nguyen
'''
import unittest
import os

from WMCore.WMBase import getTestBase

from WMCore.Storage.StageOutMgr import StageOutMgr
from WMCore.Storage.SiteLocalConfig import SiteLocalConfig, SiteConfigError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig


class StageOutMgrTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())
        os.putenv('SITECONFIG_PATH', os.getcwd())

    def testStageOutMgr(self):
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL'
        stageOutMgr = StageOutMgr()
        stageOutMgr.bypassImpl = True
        stageOutMgr_override = StageOutMgr(**{"command":"gfal2","phedex-node":"T1_US_FNAL_Disk","lfn-prefix":"root://abc/xyz"})
        stageOutMgr_override.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr(fileToStage)
        stageOutMgr.cleanSuccessfulStageOuts()
        stageOutMgr_override(fileToStage)
        stageOutMgr_override.cleanSuccessfulStageOuts()

        return


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
