#!/usr/bin/env python
#pylint: disable-msg=E1101, W6501, W0142, C0103, W0401, E1103
# W0401: I am not going to import all those functions by hand
"""
_DBSUploadPoller_t_

DBSUpload test TestDBSUpload module and the harness
"""

import os
import sys
import threading
import unittest
import time
import cProfile, pstats

from nose.plugins.attrib import attr

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMComponent.DBSUpload.DBSUploadPoller import DBSUploadPoller
from WMComponent.DBS3Buffer.DBSBufferFile  import DBSBufferFile

from WMCore.WMFactory       import WMFactory
from WMCore.DAOFactory      import DAOFactory
from WMCore.Services.UUID   import makeUUID
from WMCore.DataStructs.Run import Run

from WMCore.Agent.Configuration import Configuration
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Agent.HeartbeatAPI  import HeartbeatAPI

from WMComponent.DBSUpload.DBSInterface import DBSInterface, listAlgorithms, \
                                               listDatasetFiles, listBlocks, \
                                               listPrimaryDatasets, listProcessedDatasets

class DBSUploadTest(unittest.TestCase):
    """
    _DBSUploadTest_

    TestCase for DBSUpload module
    """

    _maxMessage = 10

    def setUp(self):
        """
        _setUp_

        setUp function for unittest
        """
        # Set constants
        self.couchDB      = "config_test"
        self.configURL    = "RANDOM;;URL;;NAME"
        self.configString = "This is a random string"

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules =
                                ["WMComponent.DBS3Buffer",
                                 'WMCore.Agent.Database'],
                                useDefault = False)
        self.testInit.setupCouch(self.couchDB, "GroupUser", "ConfigCache")

        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)
        self.buffer3Factory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                         logger = myThread.logger,
                                         dbinterface = myThread.dbi)

        locationAction = self.bufferFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet")

        # Set heartbeat
        self.componentName = 'JobSubmitter'
        self.heartbeatAPI  = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()

        # Set up a config cache
        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = self.couchDB)
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        self.testDir = self.testInit.generateWorkDir()

        psetPath = os.path.join(self.testDir, "PSet.txt")
        f = open(psetPath, 'w')
        f.write(self.configString)
        f.close()

        configCache.addConfig(newConfig = psetPath, psetHash = None)
        configCache.save()
        self.configURL = "%s;;%s;;%s" % (os.environ["COUCHURL"],
                                         self.couchDB,
                                         configCache.getCouchID())
        return

    def tearDown(self):
        """
        _tearDown_

        tearDown function for unittest
        """

        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        return

    def createConfig(self):
        """
        _createConfig_

        This creates the actual config file used by the component

        """
        config = Configuration()

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        config.section_("Agent")
        config.Agent.componentName = 'DBSUpload'
        config.Agent.useHeartbeat    = False

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")


        config.component_("DBSUpload")
        config.DBSUpload.pollInterval  = 10
        config.DBSUpload.logLevel      = 'ERROR'
        config.DBSUpload.maxThreads    = 1
        config.DBSUpload.namespace     = 'WMComponent.DBSUpload.DBSUpload'
        config.DBSUpload.componentDir  = os.path.join(os.getcwd(), 'Components')
        config.DBSUpload.workerThreads = 4

        config.section_("DBSInterface")
        config.DBSInterface.globalDBSUrl     = 'http://vocms09.cern.ch:8880/cms_dbs_int_local_xx_writer/servlet/DBSServlet'
        config.DBSInterface.globalDBSVersion = 'DBS_2_0_9'
        config.DBSInterface.DBSUrl           = 'http://vocms09.cern.ch:8880/cms_dbs_int_local_yy_writer/servlet/DBSServlet'
        config.DBSInterface.DBSVersion       = 'DBS_2_0_9'
        config.DBSInterface.MaxFilesToCommit = 10

        # addition for Alerts messaging framework, work (alerts) and control
        # channel addresses to which the component will be sending alerts
        # these are destination addresses where AlertProcessor:Receiver listens
        config.section_("Alert")
        config.Alert.address = "tcp://127.0.0.1:5557"
        config.Alert.controlAddr = "tcp://127.0.0.1:5559"
        # configure threshold of DBS upload queue size alert threshold
        # reference: trac ticket #1628
        config.DBSUpload.alertUploadQueueSize = 2000

        return config

    def injectWorkflow(self, workflowName = 'TestWorkflow',
                       taskPath = '/TestWorkflow/ReadingEvents',
                       MaxWaitTime  = 10000,
                       MaxFiles = 10,
                       MaxEvents = 250000000,
                       MaxSize = 9999999999):
        """
        _injectWorklow_

        Inject a dummy worklow in DBSBuffer for testing,
        returns the workflow ID
        """
        injectWorkflowDAO = self.buffer3Factory("InsertWorkflow")
        workflowID = injectWorkflowDAO.execute(workflowName, taskPath,
                                               MaxWaitTime, MaxFiles,
                                               MaxEvents, MaxSize)
        return workflowID

    def getFiles(self, name, tier, nFiles = 12, site = "malpaquet", workflowName = None, taskPath = None,
                 noChild = False):
        """
        Create some quick dummy test files
        """

        if workflowName is not None and taskPath is not None:
            workflowId = self.injectWorkflow(workflowName = workflowName,
                                             taskPath = taskPath)
        else:
            workflowId = self.injectWorkflow()

        files = []

        for f in range(0, nFiles):
            testFile = DBSBufferFile(lfn = '%s-%s-%i' % (name, site, f), size = 1024,
                                     events = 20, checksums = {'cksum': 1}, workflowId = workflowId)
            testFile.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = self.configURL)
            testFile.setDatasetPath("/%s/%s/%s" % (name, name, tier))
            testFile.addRun(Run( 1, *[f]))
            testFile.setGlobalTag("aGlobalTag")
            testFile.create()
            testFile.setLocation(site)
            files.append(testFile)

        if not noChild:
            testFileChild = DBSBufferFile(lfn = '%s-%s-child' %(name, site), size = 1024,
                                     events = 10, checksums = {'cksum': 1},
                                     workflowId = workflowId)
            testFileChild.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = self.configURL)
            testFileChild.setDatasetPath("/%s/%s_2/RECO" %(name, name))
            testFileChild.addRun(Run( 1, *[45]))
            testFileChild.setGlobalTag("aGlobalTag")
            testFileChild.create()
            testFileChild.setLocation(site)

            testFileChild.addParents([x['lfn'] for x in files])


        return files


    @attr('integration')
    def testA_basicUploadTest(self):
        """
        _basicUploadTest_

        Do everything simply once
        Create dataset, algo, files, blocks,
        upload them,
        mark as done, finish them, migrate them
        Also check the timeout
        """
        myThread = threading.currentThread()
        config = self.createConfig()
        self.injectWorkflow(MaxWaitTime = 3)
        config.DBSUpload.pollInterval  = 4

        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 12
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)


        # Load components that are necessary to check status
        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        dbsInterface = DBSInterface(config = config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)

        # In the first round we should create blocks for the first dataset
        # The child dataset should not be handled until the parent is uploaded
        testDBSUpload = DBSUploadPoller(config = config)
        testDBSUpload.algorithm()

        # First, see if there are any blocks
        # One in DBS, one not in DBS
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(len(result), 2)
        self.assertEqual(result, [('InGlobalDBS',), ('Open',)])

        # Check to see if datasets and algos are in local DBS
        result  = listAlgorithms(apiRef = localAPI, patternExe = name)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ExecutableName'], name)
        result  = listPrimaryDatasets(apiRef = localAPI, match = name)
        self.assertEqual(result, [name])
        result    = listProcessedDatasets(apiRef = localAPI, primary = name, dataTier = "*")

        # Then check and see that the closed block made it into local DBS
        affectedBlocks = listBlocks(apiRef = localAPI, datasetPath = datasetPath)
        if affectedBlocks[0]['OpenForWriting'] == '0':
            self.assertEqual(affectedBlocks[1]['OpenForWriting'], '1')
            self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 10)
            self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 2)
        else:
            self.assertEqual(affectedBlocks[0]['OpenForWriting'], '1')
            self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 10)
            self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 2)

        # Check to make sure all the files are in local
        result = listDatasetFiles(apiRef = localAPI, datasetPath = datasetPath)
        fileLFNs = [x['lfn'] for x in files]
        for lfn in fileLFNs:
            self.assertTrue(lfn in result)

        # Make sure the child files aren't there
        flag = False
        try:
            listDatasetFiles(apiRef = localAPI,
                             datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        except Exception, ex:
            flag = True
        self.assertTrue(flag)


        # There should be one blocks in global
        # It should have ten files and be closed
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)
        for block in result:
            self.assertEqual(block['OpenForWriting'], '0')
            self.assertTrue(block['NumberOfFiles'] in [2, 10])

        # Okay, deep breath.  First round done
        # In the second round, the second block of the parent fileset should transfer
        # Make sure that the timeout functions work
        time.sleep(10)
        testDBSUpload.algorithm()

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(len(result), 2)
        self.assertEqual(result, [('InGlobalDBS',), ('InGlobalDBS',)])

        # Check to make sure all the files are in global
        result = listDatasetFiles(apiRef = globeAPI, datasetPath = datasetPath)
        for lfn in fileLFNs:
            self.assertTrue(lfn in result)

        # Make sure the child files aren't there
        flag = False
        try:
            listDatasetFiles(apiRef = localAPI,
                             datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        except Exception, ex:
            flag = True
        self.assertTrue(flag)

        # Third round
        # Both of the parent blocks should have transferred
        # So the child block should now transfer
        testDBSUpload.algorithm()

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('InGlobalDBS',), ('Open',)])


        flag = False
        try:
            result = listDatasetFiles(apiRef = localAPI,
                                      datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        except Exception, ex:
            flag = True
        self.assertFalse(flag)

        self.assertEqual(len(result), 1)

        return


    @attr('integration')
    def testB_AlgoMigration(self):
        """
        _AlgoMigration_

        Test our ability to migrate multiple algos to global

        Do this by creating, mid-poll, two separate batches of files
        One with the same dataset but a different algo
        One with the same algo, but a different dataset
        See that they both get to global
        """
        #raise nose.SkipTest
        myThread = threading.currentThread()
        config = self.createConfig()
        self.injectWorkflow(MaxWaitTime = 20)
        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 12
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)


        # Load components that are necessary to check status
        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        dbsInterface = DBSInterface(config = config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)


        testDBSUpload = DBSUploadPoller(config = config)
        testDBSUpload.algorithm()

        # There should now be one block
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)

        # Okay, by now, the first migration should have gone through.
        # Now create a second batch of files with the same dataset
        # but a different algo.
        for i in range(0, nFiles):
            testFile = DBSBufferFile(lfn = '%s-batch2-%i' %(name, i), size = 1024,
                                     events = 20, checksums = {'cksum': 1},
                                     locations = "malpaquet")
            testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                  appFam = tier, psetHash = "GIBBERISH_PART2",
                                  configContent = self.configURL)
            testFile.setDatasetPath(datasetPath)
            testFile.addRun(Run( 1, *[46]))
            testFile.create()


        # Have to do things twice to get parents
        testDBSUpload.algorithm()
        testDBSUpload.algorithm()

        # There should now be two blocks
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 2)


        # Now create another batch of files with the original algo
        # But in a different dataset
        for i in range(0, nFiles):
            testFile = DBSBufferFile(lfn = '%s-batch3-%i' %(name, i), size = 1024,
                                     events = 20, checksums = {'cksum': 1},
                                     locations = "malpaquet")
            testFile.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                                  appFam = tier, psetHash = "GIBBERISH",
                                  configContent = self.configURL)
            testFile.setDatasetPath('/%s/%s_3/%s' % (name, name, tier))
            testFile.addRun(Run( 1, *[46]))
            testFile.create()

        # Do it twice for parentage.
        testDBSUpload.algorithm()
        testDBSUpload.algorithm()


        # There should now be one block
        result    = listBlocks(apiRef = globeAPI, datasetPath = '/%s/%s_3/%s' % (name, name, tier))
        self.assertEqual(len(result), 1)


        # Well, all the blocks got there, so we're done
        return


    @attr('integration')
    def testC_FailTest(self):
        """
        _FailTest_

        THIS TEST IS DANGEROUS!
        Figure out what happens when we trigger rollbacks
        """
        myThread = threading.currentThread()
        config = self.createConfig()
        config.DBSUpload.abortStepTwo = True

        originalOut = sys.stdout
        originalErr = sys.stderr

        dbsInterface = DBSInterface(config = config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)

        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 12
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)

        testDBSUpload = DBSUploadPoller(config = config)

        try:
            testDBSUpload.algorithm()
        except Exception, ex:
            pass

        # Aborting in step two should result in no results
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(len(result), 0)

        config.DBSUpload.abortStepTwo   = False
        config.DBSUpload.abortStepThree = True
        testDBSUpload = DBSUploadPoller(config = config)

        try:
            testDBSUpload.algorithm()
        except Exception, ex:
            pass


        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('Pending',), ('Open',)])
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_file WHERE dataset_algo = 1")[0].fetchall()
        for res in result:
            self.assertEqual(res[0], 'READY')

        config.DBSUpload.abortStepThree     = False
        self.injectWorkflow(MaxWaitTime = 300)
        testDBSUpload = DBSUploadPoller(config = config)
        testDBSUpload.algorithm()

        # After this, one block should have been uploaded, one should still be open
        # This is the result of the pending block updating, and the open block staying open
        result = myThread.dbi.processData("SELECT status, id FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS', 3L), ('Open', 4L)])

        # Check that one block got there
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['NumberOfFiles'], 10)
        self.assertEqual(result[0]['NumberOfEvents'], 200)
        self.assertEqual(result[0]['BlockSize'], 10240)

        # Check that ten files got there
        result = listDatasetFiles(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 10)

        myThread.dbi.processData("UPDATE dbsbuffer_workflow SET block_close_max_wait_time = 1")
        testDBSUpload = DBSUploadPoller(config = config)
        time.sleep(3)
        testDBSUpload.algorithm()

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('InGlobalDBS',)])

        result = listDatasetFiles(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 12)

        fileLFNs = [x['lfn'] for x in files]
        for lfn in fileLFNs:
            self.assertTrue(lfn in result)

        testDBSUpload.algorithm()
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('InGlobalDBS',), ('Open',)])

        time.sleep(5)
        testDBSUpload.algorithm()
        time.sleep(2)
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('InGlobalDBS',), ('InGlobalDBS',)])

        result = listDatasetFiles(apiRef = globeAPI,
                                  datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(result), 1)

        sys.stdout = originalOut
        sys.stderr = originalErr

        return



    @attr('integration')
    def testD_Profile(self):
        """
        _Profile_

        Profile with cProfile and time various pieces
        """
        return
        config = self.createConfig()

        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 500
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)


        testDBSUpload = DBSUploadPoller(config = config)
        cProfile.runctx("testDBSUpload.algorithm()", globals(), locals(), filename = "testStats.stat")

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(0.2)

        return

    @attr('integration')
    def testE_NoMigration(self):
        """
        _NoMigration_

        Test the DBSUpload system with no global migration
        """
        myThread = threading.currentThread()
        config = self.createConfig()
        self.injectWorkflow(MaxWaitTime = 3)
        config.DBSInterface.doGlobalMigration = False
        config.DBSUpload.pollInterval         = 4

        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 12
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)


        # Load components that are necessary to check status
        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        dbsInterface = DBSInterface(config = config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)

        # In the first round we should create blocks for the first dataset
        # The child dataset should not be handled until the parent is uploaded
        testDBSUpload = DBSUploadPoller(config = config)
        testDBSUpload.algorithm()

        # First, see if there are any blocks
        # One in DBS, one not in DBS
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(len(result), 2)
        self.assertEqual(result, [('InGlobalDBS',), ('Open',)])


        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_file WHERE dataset_algo = 1")[0].fetchall()
        for r in result:
            self.assertEqual(r[0], 'GLOBAL')


        return

    @attr('integration')
    def testF_DBSUploadQueueSizeCheckForAlerts(self):
        """
        Test will not trigger a real alert being sent unless doing some
        mocking of the methods used during DBSUploadPoller.algorithm() ->
        DBSUploadPoller.uploadBlocks() method.
        As done here, it probably can't be deterministic, yet the feature
        shall be checked.

        """
        sizeLevelToTest = 1
        myThread = threading.currentThread()
        config = self.createConfig()
        # threshold / value to check
        config.DBSUpload.alertUploadQueueSize = sizeLevelToTest

        # without this uploadBlocks method returns immediately
        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = sizeLevelToTest + 1
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)

        # load components that are necessary to check status
        # (this seems necessary, else some previous tests started failing)
        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        dbsInterface = DBSInterface(config = config)
        localAPI = dbsInterface.getAPIRef()
        globeAPI = dbsInterface.getAPIRef(globalRef = True)
        testDBSUpload = DBSUploadPoller(config)
        # this is finally where the action (alert) should be triggered from
        testDBSUpload.algorithm()

        return

    def testG_closeSettingsPerWorkflow(self):
        """
        _closeSettingsPerWorkflow_

        Test our ability to close blocks depending on settings
        configured for individual workflows.
        This unit test that doesn't require an actual DBS instance to run.
        """
        self.assertTrue(False, 'This unit test disabled since we do not have DBS2 mock')
        myThread = threading.currentThread()
        config = self.createConfig()
        config.DBSInterface.doGlobalMigration = False

        # First test, limit by number of files and timeout without new files
        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 12
        self.injectWorkflow(workflowName = name, taskPath = '/%s/Test' % name,
                            MaxFiles = 5)
        self.getFiles(name = name, tier = tier, nFiles = nFiles,
                              workflowName = name, taskPath = '/%s/Test' % name)

        # Load components that are necessary to check status
        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        # Change the DBSUploadPoller imports on runtime
        from WMComponent.DBSUpload import DBSUploadPoller as MockDBSUploadPoller
        #MockDBSUploadPoller.DBSInterface = DBS2Interface

        # In the first round we should create blocks for the first dataset
        # The child dataset should not be handled until the parent is uploaded
        # First run creates 3 blocks, 2 are closed immediately and one is open
        testDBSUpload = MockDBSUploadPoller.DBSUploadPoller(config = config)
        testDBSUpload.algorithm()
        openBlocks = dbinterface.findOpenBlocks()
        closedBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status = 'InGlobalDBS'")[0].fetchall()
        self.assertEqual(len(openBlocks), 1)
        self.assertEqual(len(closedBlocks), 2)
        globalFiles = myThread.dbi.processData("SELECT id FROM dbsbuffer_file WHERE status = 'GLOBAL'")[0].fetchall()
        notUploadedFiles = myThread.dbi.processData("SELECT * FROM dbsbuffer_file WHERE status = 'NOTUPLOADED'")[0].fetchall()
        self.assertEqual(len(globalFiles), 12)
        self.assertEqual(len(notUploadedFiles), 1)
        self.assertTrue('child' in notUploadedFiles[0][1])
        testDBSUpload.algorithm()
        openBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status != 'InGlobalDBS'")[0].fetchall()
        closedBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status = 'InGlobalDBS'")[0].fetchall()
        self.assertEqual(len(openBlocks), 2)
        self.assertEqual(len(closedBlocks), 2)
        globalFiles = myThread.dbi.processData("SELECT id FROM dbsbuffer_file WHERE status = 'GLOBAL'")[0].fetchall()
        notUploadedFiles = myThread.dbi.processData("SELECT * FROM dbsbuffer_file WHERE status = 'NOTUPLOADED'")[0].fetchall()
        self.assertEqual(len(globalFiles), 13)
        self.assertEqual(len(notUploadedFiles), 0)
        # Test the timeout feature to close blocks
        myThread.dbi.processData("UPDATE dbsbuffer_workflow SET block_close_max_wait_time = 0")
        testDBSUpload.algorithm()
        openBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status != 'InGlobalDBS'")[0].fetchall()
        closedBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status = 'InGlobalDBS'")[0].fetchall()
        self.assertEqual(len(openBlocks), 0)
        self.assertEqual(len(closedBlocks), 4)
        # Check the information that DBS received
        dbsBlocks = testDBSUpload.dbsInterface.blocks
        for dbsBlockName in dbsBlocks:
            dbsBlock = dbsBlocks[dbsBlockName]
            self.assertEqual(dbsBlock['OpenForWriting'], '0')
            self.assertTrue(dbsBlock['nFiles'] in (1,2,5))

        # Second test, limit by number of events and timeout with new files
        name = "ThisIsATest_%s" % (makeUUID())
        nFiles = 50
        self.injectWorkflow(workflowName = name, taskPath = '/%s/Test' % name,
                            MaxFiles = 45, MaxEvents = 800, MaxWaitTime = 10000)
        self.getFiles(name = name, tier = tier, nFiles = nFiles,
                              workflowName = name, taskPath = '/%s/Test' % name)
        testDBSUpload.algorithm()
        testDBSUpload.algorithm()
        openBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status != 'InGlobalDBS'")[0].fetchall()
        closedBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status = 'InGlobalDBS'")[0].fetchall()
        self.assertEqual(len(openBlocks), 2)
        self.assertEqual(len(closedBlocks), 5)
        # Throw 20 new file
        # Reset the timer such that the blocks appear to have been created 10001 seconds ago
        creationTime = int(time.time() - 10001)
        myThread.dbi.processData("UPDATE dbsbuffer_block SET create_time = %d WHERE status != 'InGlobalDBS'" % creationTime)
        self.getFiles(name = name + '2', tier = tier, nFiles = 20,
                      workflowName = name, taskPath = '/%s/Test' % name,
                      noChild = True)
        # Now a new block will have to be created as the last one timed out
        testDBSUpload.algorithm()
        openBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status != 'InGlobalDBS'")[0].fetchall()
        closedBlocks = myThread.dbi.processData("SELECT id FROM dbsbuffer_block WHERE status = 'InGlobalDBS'")[0].fetchall()
        self.assertEqual(len(openBlocks), 1)
        self.assertEqual(len(closedBlocks), 7)
        dbsBlocks = testDBSUpload.dbsInterface.blocks
        for dbsBlockName in dbsBlocks:
            dbsBlock = dbsBlocks[dbsBlockName]
            if name in dbsBlockName:
                if dbsBlock['OpenForWriting'] == '1':
                    self.assertEqual(dbsBlock['nFiles'], 20)
                else:
                    self.assertTrue(dbsBlock['events'] in (10,200,800))
                    self.assertTrue(dbsBlock['nFiles'] in (1,10,40))

        # Last test, check limitation by size
        name = "ThisIsATest_%s" % (makeUUID())
        nFiles = 10
        self.injectWorkflow(workflowName = name, taskPath = '/%s/Test' % name,
                            MaxFiles = 45, MaxEvents = 800, MaxSize = 2048)
        self.getFiles(name = name, tier = tier, nFiles = nFiles,
                              workflowName = name, taskPath = '/%s/Test' % name)
        testDBSUpload.algorithm()
        dbsBlocks = testDBSUpload.dbsInterface.blocks
        for dbsBlockName in dbsBlocks:
            dbsBlock = dbsBlocks[dbsBlockName]
            if name in dbsBlockName:
                self.assertEqual(dbsBlock['events'], 40)
                self.assertEqual(dbsBlock['nFiles'], 2)
                self.assertEqual(dbsBlock['size'], 2048)

        return

if __name__ == '__main__':
    unittest.main()
