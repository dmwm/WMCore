#!/usr/bin/env python
#pylint: disable-msg=E1101, W6501, W0142, C0103, W0401, E1103
# W0401: I am not going to import all those functions by hand


"""

DBSUpload test TestDBSUpload module and the harness

"""


import os
import sys
import threading
import time
import unittest
import cProfile, pstats
import nose

from WMComponent.DBSUpload.DBSUpload       import DBSUpload
from WMComponent.DBSUpload.DBSUploadPoller import DBSUploadPoller
#from WMComponent.DBSUpload.DBSUploadTest import DBSUploadPoller2 as DBSUploadPoller
from WMComponent.DBSUpload.DBSUploadWorker import DBSUploadWorker

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.WMFactory       import WMFactory
from WMQuality.TestInit     import TestInit
from WMCore.DAOFactory      import DAOFactory
from WMCore.Services.UUID   import makeUUID
from WMCore.DataStructs.Run import Run

from WMCore.Agent.Configuration import Configuration

from WMCore.Agent.HeartbeatAPI  import HeartbeatAPI

#from WMCore.Services.DBS.DBSReader import DBSReader

from WMComponent.DBSUpload.DBSInterface import *

from DBSAPI.dbsApi import DbsApi

class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module 
    
    Note:
      This fails if you use the in-memory syntax for sqlite 
      i.e. (DATABASE = sqlite://)
    """
    _maxMessage = 10


    def setUp(self):
        """
        _setUp_
        
        setUp function for unittest

        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = 
                                ["WMCore.ThreadPool",
                                 "WMCore.MsgService",
                                 "WMComponent.DBSBuffer.Database",
                                 'WMCore.Agent.Database'],
                                useDefault = False)
      
        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
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

        return

    def tearDown(self):
        """
        _tearDown_
        
        tearDown function for unittest
        """
        
        self.testInit.clearDatabase()

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
        config.DBSInterface.globalDBSUrl     = 'http://cmssrv49.fnal.gov:8989/DBS209P5/servlet/DBSServlet'
        config.DBSInterface.globalDBSVersion = 'DBS_2_0_9'
        config.DBSInterface.DBSUrl           = 'http://cmssrv49.fnal.gov:8989/DBS209P5_2/servlet/DBSServlet'
        config.DBSInterface.DBSVersion       = 'DBS_2_0_9'
        config.DBSInterface.DBSBlockMaxFiles = 10
        config.DBSInterface.DBSBlockMaxSize  = 9999999999
        config.DBSInterface.DBSBlockMaxTime  = 10000
        config.DBSInterface.MaxFilesToCommit = 10

        return config


    def getFiles(self, name, tier, nFiles = 12, site = "malpaquet"):
        """
        Create some quick dummy test files


        """

        files = []

        for f in range(0, nFiles):
            testFile = DBSBufferFile(lfn = '%s-%s-%i' % (name, site, f), size = 1024,
                                     events = 20, checksums = {'cksum': 1})
            testFile.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/%s/%s/%s" % (name, name, tier))
            testFile.addRun(Run( 1, *[f]))
            testFile.create()
            testFile.setLocation(site)
            files.append(testFile)


        testFileChild = DBSBufferFile(lfn = '%s-%s-child' %(name, site), size = 1024,
                                 events = 10, checksums = {'cksum': 1})
        testFileChild.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFileChild.setDatasetPath("/%s/%s_2/RECO" %(name, name))
        testFileChild.addRun(Run( 1, *[45]))
        testFileChild.create()
        testFileChild.setLocation(site)

        testFileChild.addParents([x['lfn'] for x in files])


        return files



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
        config.DBSInterface.DBSBlockMaxTime = 3
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
        config.DBSInterface.DBSBlockMaxTime = 20

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
                                  configContent = "MOREGIBBERISH")
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
                                  configContent = "MOREGIBBERISH")
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



    def testC_FailTest(self):
        """
        _FailTest_

        THIS TEST IS DANGEROUS!
        Figure out what happens when we trigger rollbacks
        """
        myThread = threading.currentThread()
        config = self.createConfig()
        config.DBSUpload.abortStepTwo = True

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
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_file")[0].fetchall()
        for res in result:
            self.assertEqual(res[0], 'NOTUPLOADED')

        config.DBSUpload.abortStepThree     = False
        config.DBSInterface.DBSBlockMaxTime = 300
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

        config.DBSInterface.DBSBlockMaxTime = 1
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

        time.sleep(3)
        testDBSUpload.algorithm()
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('InGlobalDBS',), ('InGlobalDBS',)])

        result = listDatasetFiles(apiRef = globeAPI,
                                  datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(result), 1)        

        return




    def testD_Profile(self):
        """
        _Profile_

        Profile with cProfile and time various pieces
        """
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


        

        
        

if __name__ == '__main__':
    unittest.main()
