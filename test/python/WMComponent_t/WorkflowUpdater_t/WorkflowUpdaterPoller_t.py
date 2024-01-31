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
from WMQuality.Emulators.RucioClient.MockRucioApi import MockRucioApi
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
        rdict = {'block1': rses, 'block2': rses}
        doc = updateBlockInfo(jdoc, rdict)
        for rec in doc.values():
            self.assertEqual(rec['block1']['PhEDExNodeNames'], ['rse1'])
            # block3 should be discarded by now
            self.assertEqual(len(rec), 2)
            self.assertEqual(rec['block2']['PhEDExNodeNames'], ['rse2'])

    def testCheckChanges(self):
        """
        Test checkChanges function
        """
        rdict = {}
        bdict = blockLocations(dict(self.jdoc))
        status = checkChanges(rdict, bdict)
        self.assertEqual(status, True)
        status = checkChanges(bdict, bdict)
        self.assertEqual(status, False)

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
        writePileupJson(self.sandbox1, jdict, tmpFile)

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
        # setup WorkflowUpdater
        config = Configuration()
        config.component_('WorkflowUpdater')
        config.WorkflowUpdater.rucioAccount = 'wmcore_pileup'
        config.WorkflowUpdater.rucioUrl = 'http://cms-rucio-int.cern.ch'
        config.WorkflowUpdater.rucioAuthUrl = 'https://cms-rucio-auth-int.cern.ch'
        config.WorkflowUpdater.rucioCustomScope = 'group.wmcore'
        config.WorkflowUpdater.msPileupUrl = 'https://cmsweb-testbed.cern.ch/mspileup'
        config.WorkflowUpdater.sandboxDir = '/tmp'
        obj = WorkflowUpdaterPoller(config)
        obj.rucio = MockRucioApi(config.WorkflowUpdater.rucioAccount,
                                 config.WorkflowUpdater.rucioUrl,
                                 config.WorkflowUpdater.rucioAuthUrl)
        self.logger = logging.getLogger()
        obj.logger = self.logger
        obj.logger.setLevel(logging.DEBUG)

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

        # untar our sandbox file and adjust pileup blocks
        with tarfile.open(tarFileName, 'r:bz2') as tar:
            tar.extractall(path=dstDir)
            self.logger.debug("### files in %s: %s", dstDir, os.listdir(dstDir))

            # we got spec and pileup name from self.sandbox1
            wflowSpec = "GenSimFull"
            pileup = "/RelValMinBias_14TeV/CMSSW_11_2_0_pre8-112X_mcRun3_2024_realistic_v10_forTrk-v1/GEN-SIM"
            # here we pass fake blocks name, therefore new content should be empty
            blocks = ["block#123"]

            # our new pileup workflow list
            puWflows = [{'name': wflowName, 'spec': wflowSpec, 'pileup': pileup}]

            # let's fake MSPlieup list with fake rses and blocks
            msPileupList = [{'pileupName': pileup, 'rses': ["rse1"], "blocks": blocks}]
            self.logger.debug("### run adjustJSONSpec puWflows=%s msPileupList=%s", puWflows, msPileupList)

            # perform adjustment of JSON files in sandbox tarball
            obj.adjustJSONSpec(puWflows, msPileupList)

        # at this point we should updated one of our configuration files and it should have empty content
        with tarfile.open(tarFileName, 'r:bz2') as tar:
            with tempfile.TemporaryDirectory() as tmpDir:
                tar.extractall(path=tmpDir)
                confName = os.path.join(tmpDir, 'WMSandbox/GenSimFull/cmsRun1/pileupconf.json')
                with open(confName, 'r', encoding='utf-8') as istream:
                    content = json.load(istream)
                    self.logger.debug("new content %s", content)
                    # since we passed fake blocks the new content should be empty
                    self.assertEqual(content, {"mc": {}})

        # reset logger back to original
        obj.logger.setLevel(logging.WARNING)
        self.logger.setLevel(logging.WARNING)

        # clean-up destination area
        shutil.rmtree(dstDir)


if __name__ == "__main__":
    unittest.main()
