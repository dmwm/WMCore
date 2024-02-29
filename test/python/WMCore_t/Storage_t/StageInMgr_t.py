"""
Created on Nov. 7, 2023 by Duong Nguyen
"""
import os
import unittest

from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.SiteLocalConfig import stageOutStr
from WMCore.Storage.StageInMgr import StageInMgr
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutMgr import searchRFC


class StageInMgrTest(StageInMgr):
    def stageIn(self, lfn, stageOut_rfc=None):
        """
        Given the lfn and a pair of stage out and corresponding Rucio file catalog, stageOut_rfc, or override configuration invoke the stage in
        If use override configuration self.overrideConf should contain:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred
        :param lfn: logical file name
        :param stageOut_rfc: a pair of stage out attributes and corresponding Rucio file catalog
        """
        localPfn = os.path.join(os.getcwd(), os.path.basename(lfn))
        if self.override:
            pnn = self.overrideConf['phedex-node']
            command = self.overrideConf['command']
            options = self.overrideConf['option']
            if self.overrideConf['lfn-prefix'] is None:
                msg = "Unable to match lfn to pfn during stage in because override lfn-prefix is None: \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)
            protocol = command
        else:
            if not stageOut_rfc:
                msg = "Can not perform stage in for this lfn because of missing information (stageOut_rfc is None or empty): \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            pnn = stageOut_rfc[0]['phedex-node']
            command = stageOut_rfc[0]['command']
            options = stageOut_rfc[0]['option']
            pfn = searchRFC(stageOut_rfc[1], lfn)
            protocol = stageOut_rfc[1].preferredProtocol

        if pfn == None:
            msg = "Unable to match lfn to pfn: \n  %s" % lfn
            raise StageOutFailure(msg, LFN=lfn, StageOut=stageOutStr(stageOut_rfc[0]))
        try:
            impl = retrieveStageOutImpl(command, stagein=True)
        except Exception as ex:
            msg = "Unable to retrieve impl for local stage in:\n"
            msg += "Error retrieving StageOutImpl for command named: %s\n" % (command,)
            raise StageOutFailure(msg, Command=command,
                                  LFN=lfn, ExceptionDetail=str(ex))
        impl.numRetries = self.numberOfRetries
        impl.retryPause = self.retryPauseTime

        try:
            pass
        except Exception as ex:
            msg = "Failure for stage in:\n"
            msg += str(ex)
            raise StageOutFailure(msg, Command=command, Protocol=protocol,
                                  LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)
        return localPfn


class StageInMgrUnitTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())
        os.putenv('SITECONFIG_PATH', os.getcwd())

    def testStageInMgr(self):
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL'
        stageInMgr = StageInMgrTest()
        # keep using 'phedex-node' for overrideParams (to be compatible with the whole DMWM structure?)
        stageInMgr_override = StageInMgrTest(
            **{"command": "gfal2", "phedex-node": "T1_US_FNAL_Disk", "lfn-prefix": "root://abc/xyz"})
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageInMgr(**fileToStage)
        stageInMgr_override(**fileToStage)
        return


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
