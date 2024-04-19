#!/usr/bin/env python

import os
import json
import shutil
import logging
import tarfile
import tempfile
import unittest

from WMComponent.WorkflowUpdater.WorkflowUpdaterPoller import \
    blockLocations, checkChanges, updateBlockInfo, writePileupJson, \
    findJsonSandboxFiles, extractPileupconf, WorkflowUpdaterPoller
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.WMBase import getTestBase
from WMCore.Configuration import Configuration
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInit import TestInit


class WorkflowUpdaterPollerTest(EmulatedUnitTestCase):
    """
    Test case for the WorkflowUpdaterPoller
    """

    def setUp(self):
        """
        Setup the database and logging connection. Try to create all of the
        WMBS tables.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.logger = getMSLogger(False)
        super(WorkflowUpdaterPollerTest, self).setUp()

        # setup WorkflowUpdater
        config = Configuration()
        config.component_('WorkflowUpdater')
        self.dbsUrl = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
        config.WorkflowUpdater.dbsUrl = self.dbsUrl
        config.WorkflowUpdater.rucioAccount = 'wma_test'
        config.WorkflowUpdater.rucioUrl = 'http://cms-rucio-int.cern.ch'
        config.WorkflowUpdater.rucioAuthUrl = 'https://cms-rucio-auth-int.cern.ch'
        config.WorkflowUpdater.rucioCustomScope = 'group.wmcore'
        config.WorkflowUpdater.msPileupUrl = 'https://cmsweb-testbed.cern.ch/mspileup'
        self.obj = WorkflowUpdaterPoller(config)
        self.obj.logger = self.logger

        # set data objects we will use through the test
        testData = os.path.join(getTestBase(), "WMComponent_t/WorkflowUpdater_t")
        sandbox1 = 'SC_MultiPU_Agent227_Val_240110_215719_7133-Sandbox.tar.bz2'
        self.sandbox1 = os.path.join(testData, sandbox1)
        self.jdoc = {"mc": {"block1": {"FileList": [], "NumberOfEvents": 1, "PhEDExNodeNames": ['rse1']},
                            "block2": {"FileList": [], "NumberOfEvents": 1, "PhEDExNodeNames": ['rse2']},
                            "block3": {"FileList": [], "NumberOfEvents": 1, "PhEDExNodeNames": ['rse3']}}}

    def testFindJsonSandboxFiles(self):
        """
        Test findJsonSandboxFiles function
        """
        files = findJsonSandboxFiles(self.sandbox1)
        self.assertEqual(len(files), 2)
        self.logger.info(files)
        expect = ['WMSandbox/GenSimFull/cmsRun1/pileupconf.json', 'WMSandbox/GenSimFull/cmsRun2/pileupconf.json']
        self.assertEqual(files, expect)

    def testExtractPileupconf(self):
        """
        Test extractPileupconf function
        """
        self.logger.info(self.sandbox1)
        files = findJsonSandboxFiles(self.sandbox1)
        for fname in files:
            jdoc = extractPileupconf(self.sandbox1, fname)
            keys = list(jdoc.keys())
            self.assertEqual(keys, ['mc'])
            bdict = blockLocations(jdoc)
            blocks = bdict.keys()
            self.logger.info("%s found %d block names", fname, len(blocks))
            if fname == 'WMSandbox/GenSimFull/cmsRun1/pileupconf.json':
                self.assertEqual(len(blocks), 1)
            elif fname == 'WMSandbox/GenSimFull/cmsRun2/pileupconf.json':
                self.assertEqual(len(blocks), 463)

    def testBlockLocations(self):
        """
        Test blockLocations function
        """
        bdict = blockLocations(dict(self.jdoc))
        self.assertEqual(list(bdict.keys()), ["block1", "block2", "block3"])
        self.assertEqual(bdict['block1'], ['rse1'])
        self.assertEqual(bdict['block2'], ['rse2'])
        self.assertEqual(bdict['block3'], ['rse3'])

    def testUpdataBlockInfo(self):
        """
        Test updateBlockInfo function
        """
        jdoc = dict(self.jdoc)
        rses = ['rse1', 'rse2', 'rse3']
        binfo = {'PhEDExNodeNames': rses, 'files': [], 'events': 0}
        msPUBlockLoc = {'block1': binfo, 'block2': binfo}
        doc = updateBlockInfo(jdoc, msPUBlockLoc, self.dbsUrl, self.logger)
        for rec in doc.values():
            # for block1 and block2 we expect new list of rses from blockInfo
            # block3 should be discarded by now
            self.assertEqual(len(rec), 2)
            self.assertEqual(rec['block1']['PhEDExNodeNames'], binfo)
            self.assertEqual(rec['block2']['PhEDExNodeNames'], binfo)

    def testCheckChanges(self):
        """
        Test checkChanges function
        """
        jdict = dict(self.jdoc)['mc']

        # test first use-case, i.e. block in json conf has different rses from MSPileup
        msPUBlockLoc = blockLocations(dict(self.jdoc))
        # update our dict that it will have different rses
        msPUBlockLoc['block1'] = []
        status = checkChanges(jdict, msPUBlockLoc)
        self.assertEqual(status, True)

        # test second use-case, i.e. we have extra blocks in MSPileup than present in json conf
        msPUBlockLoc = blockLocations(dict(self.jdoc))
        msPUBlockLoc['block-extra'] = []
        status = checkChanges(jdict, msPUBlockLoc)
        self.assertEqual(status, True)

    def testWritePileupJson(self):
        """
        Test writePileupJson function
        """
        testDoc = {"test": True, "int": 1, "list": [1, 2, 3]}
        jdict = {}
        tmpFile = '/tmp/WMComponent-Sandbox.tar.bz2'

        # write new content to tmpFile
        files = findJsonSandboxFiles(self.sandbox1)
        for fname in files:
            jdoc = extractPileupconf(self.sandbox1, fname)
            # we skip updating part and write our test doc
            jdict[fname] = testDoc
        self.logger.info("Write %s to %s", jdict, tmpFile)
        writePileupJson(self.sandbox1, jdict, self.logger, tmpFile)

        # now we extract from tmpFile our pileupconf.json files and make comparison of their content
        for fname in files:
            jdoc = extractPileupconf(tmpFile, fname)
            self.logger.info("Extracted %s from %s (%s)", jdoc, tmpFile, fname)
            self.assertEqual(jdoc, testDoc)

        # remove tmpFile
        os.remove(tmpFile)

    def testAdjustJSONSpec(self):
        """
        Test adjustJSONSpec function
        """
        self.obj.logger.setLevel(logging.DEBUG)

        # extract workflow name from our test tar.bz2
        fname = os.path.basename(self.sandbox1)
        wflowName = fname.replace(".tar.bz2", "").replace("-Sandbox", "")

        # copy our test tar.bz2 file to test area
        dstDir = os.path.join('/tmp', wflowName)
        self.logger.debug("### create %s", dstDir)
        if not os.path.exists(dstDir):
            os.makedirs(dstDir)
        tarFileName = os.path.join(dstDir, fname)
        self.logger.debug("### copy %s to %s", self.sandbox1, dstDir)
        shutil.copyfile(self.sandbox1, tarFileName)

        # prepare our pileup information using information from self.sandbox1
        # actual wflowSpec example
        # /storage/local/data1/cmsdataops/srv/wmagent/v2.3.0/install/wmagentpy3/WorkQueueManager/cache/wangz_task_BPH-RunIISummer20UL18GEN-00263__v1_T_240106_151823_5508/WMSandbox/WMWorkload.pkl
        # we fake it as following since we keep our stuff in /tmp area
        wflowSpec = "/tmp/SC_MultiPU_Agent227_Val_240110_215719_7133/WMSandbox/WMWorkload.pkl"

        pileup1 = "/RelValMinBias_14TeV/CMSSW_11_2_0_pre8-112X_mcRun3_2024_realistic_v10_forTrk-v1/GEN-SIM"
        pileup1Blocks = ["/RelValMinBias_14TeV/CMSSW_11_2_0_pre8-112X_mcRun3_2024_realistic_v10_forTrk-v1/GEN-SIM#b9304f4c-5efd-49e5-bb85-c1f15eb4a1ad"]
        pileup2 = "/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX"
        pileup2Blocks = ["/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX#e1c457b0-b1f5-4951-9f7b-64cd81276697",
                         "/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX#a401e768-1a28-49e8-b478-6ed7ad29b5bb"]
        pileups = {pileup1: pileup1Blocks, pileup2: pileup2Blocks}

        # untar our sandbox file and adjust pileup blocks
        with tarfile.open(tarFileName, 'r:bz2') as tar:
            tar.extractall(path=dstDir)
            self.logger.debug("### files in %s: %s", dstDir, os.listdir(dstDir))

            # here we process our sandbox content with new pileup info
            for pileup, blocks in pileups.items():
                # our new pileup workflow list
                puWflows = [{'name': wflowName, 'spec': wflowSpec, 'pileup': pileup}]

                # let's fake MSPlieup list with fake rses and blocks
                msPileupList = [{'pileupName': pileup, 'rses': ["rse1"], "blocks": blocks}]
                self.logger.debug("### unit test adjustJSONSpec puWflows=%s msPileupList=%s", puWflows, msPileupList)

                # perform adjustment of JSON files in sandbox tarball
                self.obj.adjustJSONSpec(puWflows, msPileupList)

        # at this point we should updated one of our configuration files and it should have empty content
        # here we process our sandbox content with new pileup info
        with tarfile.open(tarFileName, 'r:bz2') as tar:
            with tempfile.TemporaryDirectory() as tmpDir:
                tar.extractall(path=tmpDir)

                # test 1: in first pileup we should see rses change
                confName = os.path.join(tmpDir, 'WMSandbox/GenSimFull/cmsRun1/pileupconf.json')
                blocks = pileups[pileup1]
                # please note: the confName json files has two blocks
                # since we specified only one block in blocks list (see above)
                # we should only get one block in new content
                with open(confName, 'r', encoding='utf-8') as istream:
                    content = json.load(istream)
                    # get number of blocks in new content
                    jdict = content['mc']
                    self.logger.debug("### test confName   %s", confName)
                    self.logger.debug("### orig jsonBlocks %s", blocks)
                    self.logger.debug("### new msPUBlocks  %s", list(jdict.keys()))
                    self.assertEqual(len(jdict.keys()), 1)
                    self.assertEqual(jdict[blocks[0]]["PhEDExNodeNames"], ["rse1"])

                confName = os.path.join(tmpDir, 'WMSandbox/GenSimFull/cmsRun2/pileupconf.json')
                blocks = pileups[pileup2]
                # please note: the confName json files has two blocks
                # since we specified only one block in blocks list (see above)
                # we should only get one block in new content
                with open(confName, 'r', encoding='utf-8') as istream:
                    content = json.load(istream)
                    # get number of blocks in new content
                    jdict = content['mc']
                    self.logger.debug("### test confName   %s", confName)
                    self.logger.debug("### orig jsonBlocks %s", blocks)
                    self.logger.debug("### new msPUBlocks  %s", list(jdict.keys()))
                    # since we only requested on block in blocks list it should be discarded in new content
                    self.assertEqual(len(jdict.keys()), 2)
                    self.assertEqual(jdict[blocks[0]]["PhEDExNodeNames"], ["rse1"])
                    self.assertEqual(jdict[blocks[1]]["PhEDExNodeNames"], ["rse1"])

        # reset logger back to original
        self.obj.logger.setLevel(logging.WARNING)
        self.logger.setLevel(logging.WARNING)

        # clean-up destination area
        shutil.rmtree(dstDir)


if __name__ == "__main__":
    unittest.main()
