"""
Created on Jun 18, 2009
@author: meloam
Modified on Nov. 7, 2023 by Duong Nguyen
"""
import logging
import os
import unittest

from WMCore_t.Storage_t.DeleteMgr_t import DeleteMgrTest
from future.utils import viewitems

from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.SiteLocalConfig import stageOutStr
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutMgr import StageOutMgr, searchRFC
from WMCore.WMBase import getTestBase


class StageOutMgrTest(StageOutMgr):
    def stageOut(self, lfn, localPfn, checksums, stageOut_rfc=None):
        """
        Given the lfn and a pair of stage out and corresponding Rucio file catalog, stageOut_rfc, or override configuration invoke the stage out
        If use override configuration self.overrideConf should contain:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        phedex-node - the Name of the PNN to which the file is being xferred
        :param lfn: logical file name 
        :param localPfn: physical file name of file at local location (source) that will be staged out to another location (destination)
        :param checksums: check sum of file
        :param stageOut_rfc: a pair of stage out and corresponding Rucio file catalog
        """

        if not self.override:
            if not stageOut_rfc:
                msg = "Can not perform stage out for this lfn because of missing stage out information (stageOut_rfc is None or empty): \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)
            command = stageOut_rfc[0]['command']
            options = stageOut_rfc[0]['option']
            pfn = searchRFC(stageOut_rfc[1], lfn)
            protocol = stageOut_rfc[1].preferredProtocol
            if pfn == None:
                msg = "Unable to match lfn to pfn: \n  %s" % lfn
                raise StageOutFailure(msg, LFN=lfn, StageOut=stageOutStr(stageOut_rfc[0]))
            try:
                impl = retrieveStageOutImpl(command)
            except Exception as ex:
                msg = "Unable to retrieve impl for local stage out:\n"
                msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                    command,)
                raise StageOutFailure(msg, Command=command,
                                      LFN=lfn, ExceptionDetail=str(ex))
            impl.numRetries = self.numberOfRetries
            impl.retryPause = self.retryPauseTime

            try:
                pass
            except Exception as ex:
                msg = "Failure for stage out:\n"
                msg += str(ex)
                try:
                    import traceback
                    msg += traceback.format_exc()
                except AttributeError:
                    msg += "Traceback unavailable\n"
                raise StageOutFailure(msg, Command=command, Protocol=protocol,
                                      LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)
            return pfn

        else:
            if self.overrideConf['lfn-prefix'] is None:
                msg = "Unable to match lfn to pfn during stage out because override lfn-prefix is None: \n %s" % lfn
                raise StageOutFailure(msg, LFN=lfn)

            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)

            try:
                impl = retrieveStageOutImpl(self.overrideConf['command'])
            except Exception as ex:
                msg = "Unable to retrieve impl for override stage out:\n"
                msg += "Error retrieving StageOutImpl for command named: "
                msg += "%s\n" % self.overrideConf['command']
                raise StageOutFailure(msg, Command=self.overrideConf['command'],
                                      LFN=lfn, ExceptionDetail=str(ex))

            impl.numRetries = self.numberOfRetries
            impl.retryPause = self.retryPauseTime

            try:
                pass
            except Exception as ex:
                msg = "Failure for override stage out:\n"
                msg += str(ex)
                raise StageOutFailure(msg, Command=self.overrideConf['command'],
                                      LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)

            return pfn

    def cleanSuccessfulStageOuts(self):
        """
        In the event of a failed stage out, this method can be called to cleanup the
        files that may have previously been staged out so that the job ends in a clear state
        of failure, rather than a partial success
        """

        for lfn, fileInfo in viewitems(self.completedFiles):
            pfn = fileInfo['PFN']
            command = fileInfo['StageOutCommand']
            msg = "Cleaning out file: %s\n" % lfn
            msg += "Removing PFN: %s" % pfn
            msg += "Using command implementation: %s\n" % command
            logging.info(msg)
            # Need to use DeleteMgrTest so that the actual deletion will not proceed
            delManager = DeleteMgrTest(**self.overrideConf)
            try:
                delManager.deletePFN(pfn, lfn, command)
            except StageOutFailure as ex:
                msg = "Failed to cleanup staged out file after error:"
                msg += " %s\n%s" % (lfn, str(ex))
                logging.error(msg)


class StageOutMgrUnitTest(unittest.TestCase):

    def setUp(self):
        # shut up SiteLocalConfig
        os.putenv('CMS_PATH', os.getcwd())
        os.putenv('SITECONFIG_PATH', os.getcwd())

    def testStageOutMgr(self):
        configFileName = os.path.join(getTestBase(),
                                      "WMCore_t/Storage_t/T1_DE_KIT/JobConfig",
                                      "site-local-config.xml")
        os.environ["WMAGENT_SITE_CONFIG_OVERRIDE"] = configFileName
        os.environ['SITECONFIG_PATH'] = os.path.join(getTestBase(),
                                                     "WMCore_t/Storage_t",
                                                     "T1_DE_KIT")
        os.system(
            'cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T1_DE_KIT.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgrTest()
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageOutMgr(fileToStage)
        assert fileToStage[
                   'PFN'] == "davs://cmswebdav-kit-disk.gridka.de:2880/pnfs/gridka.de/cms/disk-only/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()

        # test override
        stageOutMgr_override = StageOutMgrTest(
            **{"command": "gfal2", "phedex-node": "T1_US_FNAL_Disk", "lfn-prefix": "root://abc/xyz"})
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageOutMgr_override(fileToStage)
        assert fileToStage['PFN'] == "root://abc/xyz/store/abc/xyz.root"
        stageOutMgr_override.cleanSuccessfulStageOuts()

        # test chained rules
        os.system(
            'cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-chainedRules.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgrTest()
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageOutMgr(fileToStage)
        assert fileToStage[
                   'PFN'] == "davs://cmswebdav-kit-tape.gridka.de:2880/pnfs/gridka.de/cms/tape/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()

        # test stage-out to another site, T2_DE_DESY
        os.system(
            'cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T2_DE_DESY.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgrTest()
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageOutMgr(fileToStage)
        assert fileToStage[
                   'PFN'] == "davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()

        # test subsite
        configFileName = os.path.join(getTestBase(),
                                      "WMCore_t/Storage_t/T1_DE_KIT/KIT-T3/JobConfig",
                                      "site-local-config.xml")
        os.environ["WMAGENT_SITE_CONFIG_OVERRIDE"] = configFileName
        os.environ['SITECONFIG_PATH'] = os.path.join(getTestBase(),
                                                     "WMCore_t/Storage_t",
                                                     "T1_DE_KIT/KIT-T3")
        os.system(
            'cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T1_DE_KIT.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgrTest()
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageOutMgr(fileToStage)
        assert fileToStage[
                   'PFN'] == "davs://cmswebdav-kit-disk.gridka.de:2880/pnfs/gridka.de/cms/disk-only/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()

        # test subsite with stage-out to another site T2_DE_DESY
        os.system(
            'cp $SITECONFIG_PATH/JobConfig/site-local-config-testStageOut-T2_DE_DESY.xml $SITECONFIG_PATH/JobConfig/site-local-config.xml')
        stageOutMgr = StageOutMgrTest()
        fileToStage = {'LFN': '/store/abc/xyz.root', 'PFN': ''}
        stageOutMgr(fileToStage)
        assert fileToStage[
                   'PFN'] == "davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/abc/xyz.root"
        stageOutMgr.cleanSuccessfulStageOuts()

        return


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
