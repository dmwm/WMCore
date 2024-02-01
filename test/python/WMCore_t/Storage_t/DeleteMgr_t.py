'''
Created on Jun 18, 2009

@author: meloam

Modified on Nov. 7, 2023 by Duong Nguyen
'''
import unittest
import os

from WMCore.WMBase import getTestBase
#need this otherwise retrieveStageOutImpl fails
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.SiteLocalConfig import SiteLocalConfig, SiteConfigError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

class DeleteMgrTest(DeleteMgr):
    def deletePFN(self, pfn, lfn, command):
        """
        Delete the given PFN. This method does not include the file removal execution (impl.removeFile). Remember to transfer new changes in deletePFN of DeleteMgr to here if you want to test them 
        :param pfn: file physical file name
        :param lfn: file logical file name 
        :param command: command to be executed
        """
        try:
            impl = retrieveStageOutImpl(command)
        except Exception as ex:
            msg = "Unable to retrieve impl for file deletion %s \n" % (pfn)
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime
        
        try:
            pass
        except Exception as ex:
            self.logger.error("Failed to delete file: %s", pfn)
            ex.addInfo(Protocol=command, LFN=lfn, TargetPFN=pfn)
            raise ex

        return pfn 

class DeleteMgrUnitTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())
        os.putenv('SITECONFIG_PATH', os.getcwd())

    def testDeleteMgr(self):
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL'
        deleteMgr = DeleteMgrTest()
        #keep using 'phedex-node' for overrideParams (to be compatible with the whole DMWM structure?)
        deleteMgr_override = DeleteMgrTest(**{"command":"gfal2","phedex-node":"T1_US_FNAL_Disk","lfn-prefix":"root://abc/xyz"})
        fileToDelete = {'LFN':'/store/abc/xyz.root'}
        deleteMgr(fileToDelete)
        deleteMgr_override(fileToDelete)

        return


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
