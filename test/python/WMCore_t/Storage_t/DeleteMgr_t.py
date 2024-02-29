"""
Created on Jun 18, 2009

@author: meloam

Modified on Nov. 7, 2023 by Duong Nguyen
"""
import os
import unittest

from WMCore.Storage.DeleteMgr import DeleteMgr
# need this otherwise retrieveStageOutImpl fails
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.StageOutError import StageOutFailure


class DeleteMgrTest(DeleteMgr):
    def deletePFN(self, pfn, lfn, command):
        """
        Delete the given PFN
        """
        try:
            impl = retrieveStageOutImpl(command)
        except Exception as ex:
            msg = "Unable to retrieve impl for file deletion in %s\n" % (pfn)
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            pass
        except Exception as ex:
            self.logger.exception("Failed to delete file: %s", pfn)
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
        # keep using 'phedex-node' for overrideParams (to be compatible with the whole DMWM structure?)
        deleteMgr_override = DeleteMgrTest(
            **{"command": "gfal2", "phedex-node": "T1_US_FNAL_Disk", "lfn-prefix": "root://abc/xyz"})
        fileToDelete = {'LFN': '/store/abc/xyz.root'}
        deleteMgr(fileToDelete)
        deleteMgr_override(fileToDelete)

        return


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
