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
        os.environ['SITECONFIG_PATH'] = os.path.join(getTestBase(),
                                          "WMCore_t/Storage_t",
                                          "T1_DE_KIT")
        os.system('cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T1_DE_KIT.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgr()
        stageOutMgr.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr(fileToStage)
        assert fileToStage['PFN']=="davs://cmswebdav-kit-disk.gridka.de:2880/pnfs/gridka.de/cms/disk-only/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()
        
        #test override
        stageOutMgr_override = StageOutMgr(**{"command":"gfal2","phedex-node":"T1_US_FNAL_Disk","lfn-prefix":"root://abc/xyz"})
        stageOutMgr_override.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr_override(fileToStage)
        assert fileToStage['PFN']=="root://abc/xyz/store/abc/xyz.root"
        stageOutMgr_override.cleanSuccessfulStageOuts()
        
        #test chained rules
        os.system('cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-chainedRules.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgr()
        stageOutMgr.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr(fileToStage)
        assert fileToStage['PFN']=="davs://cmswebdav-kit-tape.gridka.de:2880/pnfs/gridka.de/cms/tape/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()
        
        #test stage-out to another site, T2_DE_DESY
        os.system('cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T2_DE_DESY.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgr()
        stageOutMgr.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr(fileToStage)
        assert fileToStage['PFN']=="davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()
        
        #test subsite
        os.environ['SITECONFIG_PATH'] = os.path.join(getTestBase(),
                                          "WMCore_t/Storage_t",
                                          "T1_DE_KIT/KIT-T3")
        os.system('cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T1_DE_KIT.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgr()
        stageOutMgr.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr(fileToStage)
        assert fileToStage['PFN']=="davs://cmswebdav-kit-disk.gridka.de:2880/pnfs/gridka.de/cms/disk-only/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()
        
        #test subsite with stage-out to another site T2_DE_DESY
        os.system('cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T2_DE_DESY.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgr()
        stageOutMgr.bypassImpl = True
        fileToStage = {'LFN':'/store/abc/xyz.root','PFN':''}
        stageOutMgr(fileToStage)
        assert fileToStage['PFN']=="davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()
        return

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
