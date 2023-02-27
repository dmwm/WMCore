#!/usr/bin/env python
# pylint: disable=E1101, C0103, W0401, E1103
# W0401: I am not going to import all those functions by hand
"""

DBSUpload test TestDBSUpload module and the harness

"""
from __future__ import division

import json
import os
import threading
import time
import unittest
from tempfile import mkstemp

from dbs.apis.dbsClient import DbsApi
from nose.plugins.attrib import attr

from WMComponent.DBS3Buffer.DBSBufferBlock import DBSBufferBlock
from WMComponent.DBS3Buffer.DBSBufferDataset import DBSBufferDataset
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil
from WMComponent.DBS3Buffer.DBSUploadPoller import DBSUploadPoller, parseDBSException
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.DBSClient.DBS3API import DbsApi as MockDbsApi
from WMQuality.TestInit import TestInit


class MyTestException(Exception):

    def __init__(self, errorMsg=None):
        super(MyTestException, self).__init__(errorMsg)
        if errorMsg is not None:
            self.reason = errorMsg


class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module

    """

    def setUp(self):
        """
        _setUp_

        setUp function for unittest

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMComponent.DBS3Buffer"],
                                useDefault=False)
        self.testDir = self.testInit.generateWorkDir(deleteOnDestruction=False)
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                        logger=myThread.logger,
                                        dbinterface=myThread.dbi)

        locationAction = self.bufferFactory(classname="DBSBufferFiles.AddLocation")
        locationAction.execute(siteName="se1.cern.ch")
        locationAction.execute(siteName="se1.fnal.gov")
        locationAction.execute(siteName="malpaquet")
        self.dbsUrl = "https://localhost:1443/dbs/dev/global/DBSWriter"
        self.dbsApi = None
        return

    def tearDown(self):
        """
        _tearDown_

        tearDown function for unittest
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        EmulatorSetup.deleteConfig(self.configFile)

        return

    def getConfig(self):
        """
        _getConfig_

        This creates the actual config file used by the component.
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        config.section_("Agent")
        config.Agent.componentName = 'DBSUpload'
        config.Agent.useHeartbeat = False

        # Now the CoreDatabase information
        # This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        config.component_("DBS3Upload")
        config.DBS3Upload.pollInterval = 10
        config.DBS3Upload.logLevel = 'DEBUG'
        # config.DBS3Upload.dbsUrl           = "https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSWriter"
        # config.DBS3Upload.dbsUrl           = "https://dbs3-dev01.cern.ch/dbs/prod/global/DBSWriter"
        config.DBS3Upload.dbsUrl = self.dbsUrl
        config.DBS3Upload.namespace = 'WMComponent.DBS3Buffer.DBS3Upload'
        config.DBS3Upload.componentDir = os.path.join(os.getcwd(), 'Components')
        config.DBS3Upload.nProcesses = 1
        config.DBS3Upload.dbsWaitTime = 0.1
        config.DBS3Upload.datasetType = "VALID"

        # added to skip the StepChain parentage setting test
        config.component_("Tier0Feeder")
        return config

    def createParentFiles(self, acqEra, nFiles=10,
                          workflowName='TestWorkload',
                          taskPath='/TestWorkload/DataTest'):
        """
        _createParentFiles_

        Create several parentless files in DBSBuffer.  This simulates raw files
        in the T0.
        """
        workflowId = self.injectWorkflow(workflowName=workflowName,
                                         taskPath=taskPath)
        parentlessFiles = []

        baseLFN = "/store/data/%s/Cosmics/RAW/v1/000/143/316/" % acqEra
        for i in range(nFiles):
            testFile = DBSBufferFile(lfn=baseLFN + makeUUID() + ".root", size=1024,
                                     events=20, checksums={"cksum": 1},
                                     workflowId=workflowId)
            testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_3_1_1",
                                  appFam="RAW", psetHash="GIBBERISH",
                                  configContent="MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-v1/RAW" % acqEra)

            testFile['block_close_max_wait_time'] = 1000000
            testFile['block_close_max_events'] = 1000000
            testFile['block_close_max_size'] = 1000000
            testFile['block_close_max_files'] = 1000000

            lumis = []
            for j in range(10):
                lumis.append((i * 10) + j)
            testFile.addRun(Run(143316, *lumis))

            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("1")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation("malpaquet")
            parentlessFiles.append(testFile)

        return parentlessFiles

    def createFilesWithChildren(self, moreParentFiles, acqEra):
        """
        _createFilesWithChildren_

        Create several parentless files and then create child files.
        """
        parentFiles = []
        childFiles = []

        baseLFN = "/store/data/%s/Cosmics/RAW/v1/000/143/316/" % acqEra
        for i in range(10):
            testFile = DBSBufferFile(lfn=baseLFN + makeUUID() + ".root", size=1024,
                                     events=20, checksums={"cksum": 1})
            testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_3_1_1",
                                  appFam="RAW", psetHash="GIBBERISH",
                                  configContent="MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-v1/RAW" % acqEra)

            testFile['block_close_max_wait_time'] = 1000000
            testFile['block_close_max_events'] = 1000000
            testFile['block_close_max_size'] = 1000000
            testFile['block_close_max_files'] = 1000000

            lumis = []
            for j in range(10):
                lumis.append((i * 10) + j)
            testFile.addRun(Run(143316, *lumis))

            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("1")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation("malpaquet")
            parentFiles.append(testFile)

        baseLFN = "/store/data/%s/Cosmics/RECO/v1/000/143/316/" % acqEra
        for i in range(5):
            testFile = DBSBufferFile(lfn=baseLFN + makeUUID() + ".root", size=1024,
                                     events=20, checksums={"cksum": 1})
            testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_3_1_1",
                                  appFam="RECO", psetHash="GIBBERISH",
                                  configContent="MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-v1/RECO" % acqEra)

            testFile['block_close_max_wait_time'] = 1000000
            testFile['block_close_max_events'] = 1000000
            testFile['block_close_max_size'] = 1000000
            testFile['block_close_max_files'] = 1000000

            lumis = []
            for j in range(20):
                lumis.append((i * 20) + j)
            testFile.addRun(Run(143316, *lumis))

            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("1")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation("malpaquet")
            testFile.addParents([parentFiles[i * 2]["lfn"],
                                 parentFiles[i * 2 + 1]["lfn"]])
            testFile.addParents([moreParentFiles[i * 2]["lfn"],
                                 moreParentFiles[i * 2 + 1]["lfn"]])
            childFiles.append(testFile)

        return parentFiles, childFiles

    def verifyData(self, datasetName, files):
        """
        _verifyData_

        Verify the data in DBS3 matches what was uploaded to it.
        """
        (dummy, dummyPrimary, processed, tier) = datasetName.split("/")
        (acqEra, dummyProcVer) = processed.split("-")

        result = self.dbsApi.listDatasets(dataset=datasetName, detail=True,
                                          dataset_access_type="VALID")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data_tier_name"], tier,
                         "Error: Wrong data tier.")
        self.assertEqual(result[0]["processing_version"], 1,
                         "Error: Wrong processing version.")
        self.assertEqual(result[0]["acquisition_era_name"], acqEra,
                         "Error: Wrong acquisition era.")
        self.assertEqual(result[0]["physics_group_name"], "NoGroup",
                         "Error: Wrong physics group name.")

        results = self.dbsApi.listBlocks(dataset=datasetName, detail=True)
        self.assertEqual(len(results), len(files) / 5,
                         "Error: Wrong number of blocks.")
        for result in results:
            self.assertEqual(result["block_size"], 5120,
                             "Error: Wrong block size.")
            self.assertEqual(result["file_count"], 5,
                             "Error: Wrong number of files.")
            self.assertEqual(result["open_for_writing"], 0,
                             "Error: Block should be closed.")

        results = self.dbsApi.listFileArray(dataset=datasetName, detail=True)
        for result in results:
            fileObj = None
            for fileObj in files:
                if fileObj["lfn"] == result["logical_file_name"]:
                    break
            else:
                fileObj = None

            self.assertTrue(fileObj is not None, "Error: File not found.")
            self.assertEqual(fileObj["size"], result["file_size"],
                             "Error: File size mismatch.")
            self.assertEqual(fileObj["events"], result["event_count"],
                             "Error: Event count mismatch.")

            dbsParents = self.dbsApi.listFileParents(logical_file_name=fileObj["lfn"])
            if len(dbsParents) == 0:
                self.assertEqual(len(fileObj["parents"]), 0,
                                 "Error: Missing parents.")
            else:
                for dbsParent in dbsParents[0]["parent_logical_file_name"]:
                    fileParent = None
                    for fileParent in fileObj["parents"]:
                        if fileParent["lfn"] == dbsParent:
                            break
                    else:
                        fileParent = None

                    self.assertTrue(fileParent is not None, "Error: Missing parent.")
                self.assertEqual(len(fileObj["parents"]), len(dbsParents[0]["parent_logical_file_name"]),
                                 "Error: Wrong number of parents.")

            runLumis = self.dbsApi.listFileLumiArray(logical_file_name=fileObj["lfn"])
            for runLumi in runLumis:
                fileRun = None
                for fileRun in fileObj["runs"]:
                    if fileRun.run == runLumi["run_num"]:
                        for lumi in runLumi["lumi_section_num"]:
                            self.assertTrue(lumi in fileRun.lumis,
                                            "Error: Missing lumis.")
                        break
                else:
                    fileRun = None

                self.assertTrue(fileRun is not None, "Error: Missing run.")

        self.assertEqual(len(files), len(results), "Error: Files missing.")
        return

    def injectWorkflow(self, workflowName='TestWorkflow',
                       taskPath='/TestWorkflow/ReadingEvents',
                       MaxWaitTime=1,
                       MaxFiles=5,
                       MaxEvents=250000000,
                       MaxSize=99999000000999999):
        """
        _injectWorklow_

        Inject a dummy worklow in DBSBuffer for testing,
        returns the workflow ID
        """
        injectWorkflowDAO = self.bufferFactory("InsertWorkflow")
        workflowID = injectWorkflowDAO.execute(workflowName, taskPath,
                                               MaxWaitTime, MaxFiles,
                                               MaxEvents, MaxSize)
        return workflowID

    @attr("integration")
    def testBasicUpload(self):
        """
        _testBasicUpload_

        Verify that we can successfully upload to DBS3.  Also verify that the
        uploader correctly handles files parentage when uploading.
        """
        self.dbsApi = DbsApi(url=self.dbsUrl)
        config = self.getConfig()
        dbsUploader = DBSUploadPoller(config=config)

        # First test verifies that uploader will poll and then not do anything
        # as the database is empty.
        dbsUploader.algorithm()

        acqEra = "Summer%s" % (int(time.time()))
        parentFiles = self.createParentFiles(acqEra)

        # The algorithm needs to be run twice.  On the first iteration it will
        # create all the blocks and upload one.  On the second iteration it will
        # timeout and upload the second block.
        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        # Verify the files made it into DBS3.
        self.verifyData(parentFiles[0]["datasetPath"], parentFiles)

        # Inject some more parent files and some child files into DBSBuffer.
        # Run the uploader twice, only the parent files should be added to DBS3.
        (moreParentFiles, childFiles) = \
            self.createFilesWithChildren(parentFiles, acqEra)
        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(parentFiles[0]["datasetPath"],
                        parentFiles + moreParentFiles)

        # Run the uploader another two times to upload the child files.  Verify
        # that the child files were uploaded.
        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(childFiles[0]["datasetPath"], childFiles)
        return

    @attr("integration")
    def testDualUpload(self):
        """
        _testDualUpload_

        Verify that the dual upload mode works correctly.
        """
        self.dbsApi = DbsApi(url=self.dbsUrl)
        config = self.getConfig()
        dbsUploader = DBSUploadPoller(config=config)
        dbsUtil = DBSBufferUtil()

        # First test verifies that uploader will poll and then not do anything
        # as the database is empty.
        dbsUploader.algorithm()

        acqEra = "Summer%s" % (int(time.time()))
        parentFiles = self.createParentFiles(acqEra)
        (moreParentFiles, childFiles) = \
            self.createFilesWithChildren(parentFiles, acqEra)

        allFiles = parentFiles + moreParentFiles
        allBlocks = []
        for i in range(4):
            DBSBufferDataset(parentFiles[0]["datasetPath"]).create()
            blockName = parentFiles[0]["datasetPath"] + "#" + makeUUID()
            dbsBlock = DBSBufferBlock(blockName,
                                      location="malpaquet",
                                      datasetpath=None)
            dbsBlock.status = "Open"
            dbsBlock.setDataset(parentFiles[0]["datasetPath"], 'data', 'VALID')
            dbsUtil.createBlocks([dbsBlock])
            for fileObj in allFiles[i * 5: (i * 5) + 5]:
                dbsBlock.addFile(fileObj, 'data', 'VALID')
                dbsUtil.setBlockFiles({"block": blockName, "filelfn": fileObj["lfn"]})
                if i < 2:
                    dbsBlock.status = "InDBS"
                dbsUtil.updateBlocks([dbsBlock])
            dbsUtil.updateFileStatus([dbsBlock], "InDBS")
            allBlocks.append(dbsBlock)

        DBSBufferDataset(childFiles[0]["datasetPath"]).create()
        blockName = childFiles[0]["datasetPath"] + "#" + makeUUID()
        dbsBlock = DBSBufferBlock(blockName,
                                  location="malpaquet",
                                  datasetpath=None)
        dbsBlock.status = "InDBS"
        dbsBlock.setDataset(childFiles[0]["datasetPath"], 'data', 'VALID')
        dbsUtil.createBlocks([dbsBlock])
        for fileObj in childFiles:
            dbsBlock.addFile(fileObj, 'data', 'VALID')
            dbsUtil.setBlockFiles({"block": blockName, "filelfn": fileObj["lfn"]})

        dbsUtil.updateFileStatus([dbsBlock], "InDBS")

        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(parentFiles[0]["datasetPath"], parentFiles)

        # Change the status of the rest of the parent blocks so we can upload
        # them and the children.
        for dbsBlock in allBlocks:
            dbsBlock.status = "InDBS"
            dbsUtil.updateBlocks([dbsBlock])

        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(parentFiles[0]["datasetPath"], parentFiles + moreParentFiles)

        # Run the uploader one more time to upload the children.
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(childFiles[0]["datasetPath"], childFiles)
        return

    def testCloseSettingsPerWorkflow(self):
        """
        _testCloseSettingsPerWorkflow_

        Test the block closing mechanics in the DBS3 uploader,
        this uses a fake dbs api to avoid reliance on external services.
        """
        # Signal trapExit that we are a friend
        os.environ["DONT_TRAP_EXIT"] = "True"
        try:
            # Monkey patch the imports of DbsApi
            from WMComponent.DBS3Buffer import DBSUploadPoller as MockDBSUploadPoller
            MockDBSUploadPoller.DbsApi = MockDbsApi

            # Set the poller and the dbsUtil for verification
            myThread = threading.currentThread()
            (_, dbsFilePath) = mkstemp(dir=self.testDir)
            self.dbsUrl = dbsFilePath
            config = self.getConfig()
            dbsUploader = MockDBSUploadPoller.DBSUploadPoller(config=config)
            dbsUtil = DBSBufferUtil()

            # First test is event based limits and timeout with no new files.
            # Set the files and workflow
            acqEra = "TropicalSeason%s" % (int(time.time()))
            workflowName = 'TestWorkload%s' % (int(time.time()))
            taskPath = '/%s/TestProcessing' % workflowName
            self.injectWorkflow(workflowName, taskPath,
                                MaxWaitTime=2, MaxFiles=100,
                                MaxEvents=150)
            self.createParentFiles(acqEra, nFiles=20,
                                   workflowName=workflowName,
                                   taskPath=taskPath)

            # The algorithm needs to be run twice.  On the first iteration it will
            # create all the blocks and upload one with less than 150 events.
            # On the second iteration the second block is uploaded.
            dbsUploader.algorithm()
            dbsUploader.checkBlocks()
            openBlocks = dbsUtil.findOpenBlocks()
            self.assertEqual(len(openBlocks), 1)
            globalFiles = myThread.dbi.processData("SELECT id FROM dbsbuffer_file WHERE status = 'InDBS'")[0].fetchall()
            notUploadedFiles = myThread.dbi.processData("SELECT id FROM dbsbuffer_file WHERE status = 'NOTUPLOADED'")[
                0].fetchall()
            self.assertEqual(len(globalFiles), 14)
            self.assertEqual(len(notUploadedFiles), 6)
            # Check the fake DBS for data
            fakeDBS = open(self.dbsUrl, 'r')
            fakeDBSInfo = json.load(fakeDBS)
            fakeDBS.close()
            self.assertEqual(len(fakeDBSInfo), 2)
            for block in fakeDBSInfo:
                self.assertTrue('block_events' not in block['block'])
                self.assertEqual(block['block']['file_count'], 7)
                self.assertEqual(block['block']['open_for_writing'], 0)
                self.assertTrue('close_settings' not in block)
            time.sleep(3)
            dbsUploader.algorithm()
            dbsUploader.checkBlocks()
            openBlocks = dbsUtil.findOpenBlocks()
            self.assertEqual(len(openBlocks), 0)
            fakeDBS = open(self.dbsUrl, 'r')
            fakeDBSInfo = json.load(fakeDBS)
            fakeDBS.close()
            self.assertEqual(len(fakeDBSInfo), 3)
            for block in fakeDBSInfo:
                if block['block']['file_count'] != 6:
                    self.assertEqual(block['block']['file_count'], 7)
                self.assertTrue('block_events' not in block['block'])
                self.assertEqual(block['block']['open_for_writing'], 0)
                self.assertTrue('close_settings' not in block)

            # Now check the limit by size and timeout with new files
            acqEra = "TropicalSeason%s" % (int(time.time()))
            workflowName = 'TestWorkload%s' % (int(time.time()))
            taskPath = '/%s/TestProcessing' % workflowName
            self.injectWorkflow(workflowName, taskPath,
                                MaxWaitTime=2, MaxFiles=5,
                                MaxEvents=200000000)
            self.createParentFiles(acqEra, nFiles=16,
                                   workflowName=workflowName,
                                   taskPath=taskPath)
            dbsUploader.algorithm()
            dbsUploader.checkBlocks()
            openBlocks = dbsUtil.findOpenBlocks()
            self.assertEqual(len(openBlocks), 1)
            fakeDBS = open(self.dbsUrl, 'r')
            fakeDBSInfo = json.load(fakeDBS)
            fakeDBS.close()
            self.assertEqual(len(fakeDBSInfo), 6)
            for block in fakeDBSInfo:
                if acqEra in block['block']['block_name']:
                    self.assertEqual(block['block']['file_count'], 5)
                self.assertTrue('block_events' not in block['block'])
                self.assertTrue('close_settings' not in block)
                self.assertEqual(block['block']['open_for_writing'], 0)

            # Put more files, they will go into the same block and then it will be closed
            # after timeout
            time.sleep(3)
            self.createParentFiles(acqEra, nFiles=3,
                                   workflowName=workflowName,
                                   taskPath=taskPath)
            dbsUploader.algorithm()
            dbsUploader.checkBlocks()
            openBlocks = dbsUtil.findOpenBlocks()
            self.assertEqual(len(openBlocks), 0)
            fakeDBS = open(self.dbsUrl, 'r')
            fakeDBSInfo = json.load(fakeDBS)
            fakeDBS.close()
            self.assertEqual(len(fakeDBSInfo), 7)
            for block in fakeDBSInfo:
                if acqEra in block['block']['block_name']:
                    if block['block']['file_count'] < 5:
                        self.assertEqual(block['block']['file_count'], 4)
                    else:
                        self.assertEqual(block['block']['file_count'], 5)
                self.assertTrue('block_events' not in block['block'])
                self.assertEqual(block['block']['open_for_writing'], 0)
                self.assertTrue('close_settings' not in block)

            # Finally test size limits
            acqEra = "TropicalSeason%s" % (int(time.time()))
            workflowName = 'TestWorkload%s' % (int(time.time()))
            taskPath = '/%s/TestProcessing' % workflowName
            self.injectWorkflow(workflowName, taskPath,
                                MaxWaitTime=1, MaxFiles=500,
                                MaxEvents=200000000, MaxSize=2048)
            self.createParentFiles(acqEra, nFiles=7,
                                   workflowName=workflowName,
                                   taskPath=taskPath)
            dbsUploader.algorithm()
            dbsUploader.checkBlocks()
            time.sleep(2)
            dbsUploader.algorithm()
            dbsUploader.checkBlocks()

            self.assertEqual(len(openBlocks), 0)
            fakeDBS = open(self.dbsUrl, 'r')
            fakeDBSInfo = json.load(fakeDBS)
            fakeDBS.close()
            self.assertEqual(len(fakeDBSInfo), 11)
            for block in fakeDBSInfo:
                if acqEra in block['block']['block_name']:
                    if block['block']['file_count'] != 1:
                        self.assertEqual(block['block']['block_size'], 2048)
                        self.assertEqual(block['block']['file_count'], 2)
                self.assertTrue('block_events' not in block['block'])
                self.assertEqual(block['block']['open_for_writing'], 0)
                self.assertTrue('close_settings' not in block)
        except Exception as ex:
            self.fail("We failed at some point in the test: %s" % str(ex))
        finally:
            # We don't trust anyone else with _exit
            del os.environ["DONT_TRAP_EXIT"]
        return

    def testParseDBSException(self):
        """
        test parseDBSException function to parse DBS Go-based server exception message
        """
        exc = """[{"error":{"reason":"DBSError Code:110 Description:DBS DB insert record error Function:dbs.bulkblocks.insertFilesViaChunks Message: Error: concurrency error","message":"7cf3dee6307fdaa1f72d0dc03551fd0e6665f32114d752c37819c238cef7a231 unable to insert files, error DBSError Code:110 Description:DBS DB insert record error Function:dbs.bulkblocks.insertFilesViaChunks Message: Error: concurrency error","function":"dbs.bulkblocks.InsertBulkBlocksConcurrently","code":110},"http":{"method":"POST","code":400,"timestamp":"2022-05-27 19:21:45.67710595 +0000 UTC m=+191328.489143316","path":"/dbs/prod/global/DBSWriter/bulkblocks","user_agent":"DBSClient/Unknown/","x_forwarded_host":"dbs-prod.cern.ch","x_forwarded_for":"137.138.157.32","remote_addr":"137.138.63.204:39950"},"exception":400,"type":"HTTPError","message":"DBSError Code:110 Description:DBS DB insert record error Function:dbs.bulkblocks.InsertBulkBlocksConcurrently Message:7cf3dee6307fdaa1f72d0dc03551fd0e6665f32114d752c37819c238cef7a231 unable to insert files, error DBSError Code:110 Description:DBS DB insert record error Function:dbs.bulkblocks.insertFilesViaChunks Message: Error: concurrency error Error: nested DBSError Code:110 Description:DBS DB insert record error Function:dbs.bulkblocks.insertFilesViaChunks Message: Error: concurrency error"}]"""
        data = parseDBSException(exc)
        expect = 'DBSError Code:110 Description:DBS DB insert record error Function:dbs.bulkblocks.insertFilesViaChunks Message: Error: concurrency error'
        self.assertEqual(data, expect)

        # test another type of exception
        exc = Exception()
        data = parseDBSException(exc)
        self.assertEqual(data, exc)
        self.assertIs(isinstance(exc, Exception), True)


if __name__ == '__main__':
    unittest.main()
