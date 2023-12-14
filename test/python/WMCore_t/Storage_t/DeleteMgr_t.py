'''
Created on Jun 18, 2009

@author: meloam

Modified on Nov. 7, 2023 by Duong Nguyen
'''
import unittest
import os

from WMCore.WMBase import getTestBase

from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.SiteLocalConfig import SiteLocalConfig, SiteConfigError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig


class DeleteMgrTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())
        os.putenv('SITECONFIG_PATH', os.getcwd())

    def testDeleteMgr(self):
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL'
        deleteMgr = DeleteMgr()
        deleteMgr.bypassImpl = True
        #keep using 'phedex-node' for overrideParams (to be compatible with the whole DMWM structure?)
        deleteMgr_override = DeleteMgr(**{"command":"gfal2","phedex-node":"T1_US_FNAL_Disk","lfn-prefix":"root://abc/xyz"})
        deleteMgr_override.bypassImpl = True
        fileToDelete = {'LFN':'/store/abc/xyz.root'}
        deleteMgr(fileToDelete)
        deleteMgr_override(fileToDelete)

        return


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
