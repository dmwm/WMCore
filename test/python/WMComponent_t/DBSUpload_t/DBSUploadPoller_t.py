#!/usr/bin/env python
#pylint: disable-msg=E1101, W6501, W0142, C0103, W0401, E1103
# W0401: I am not going to import all those functions by hand


"""

DBSUpload test TestDBSUpload module and the harness

"""





import os
import threading
import time
import unittest
import cProfile, pstats

from WMComponent.DBSUpload.DBSUpload       import DBSUpload
from WMComponent.DBSUpload.DBSUploadPoller import DBSUploadPoller
from WMComponent.DBSUpload.DBSUploadWorker import DBSUploadWorker

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.WMFactory       import WMFactory
from WMQuality.TestInit     import TestInit
from WMCore.DAOFactory      import DAOFactory
from WMCore.Services.UUID   import makeUUID
from WMCore.DataStructs.Run import Run

from WMCore.Agent.Configuration import Configuration

#from WMCore.Services.DBS.DBSReader import DBSReader

from WMComponent.DBSUpload.DBSInterface import *

from DBSAPI.dbsApi import DbsApi
#import nose


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
        #raise Exception, "this test hangs buildbot. hard. someone (maybe me) needs to make sure that DBS is accessible";
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.testInit.clearDatabase(modules = ["WMCore.ThreadPool", "WMCore.MsgService"])
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

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")


        config.component_("DBSUpload")
        config.DBSUpload.pollInterval  = 10
        config.DBSUpload.logLevel      = 'DEBUG'
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

        #return

        myThread = threading.currentThread()
        config = self.createConfig()
        config.DBSInterface.DBSBlockMaxTime = 20
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


        testDBSUpload = DBSUpload(config = config)
        testDBSUpload.prepareToStart()
        #myThread.workerThreadManager.terminateWorkers()

        time.sleep(10)
        
        # First do the DBS checks
        # Check datasets and algos
        # Then files and blocks
        # Then block migration

        # Check to see if datasets and algos are in local DBS
        result  = listAlgorithms(apiRef = localAPI, patternExe = name)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ExecutableName'], name)
        result  = listPrimaryDatasets(apiRef = localAPI, match = name)
        self.assertEqual(result, [name])
        result    = listProcessedDatasets(apiRef = localAPI, primary = name, dataTier = "*")
        #self.assertEqual(result, [name, '%s_2' % (name)])


        # Check that files are in local DBS
        affectedBlocks = listBlocks(apiRef = localAPI, datasetPath = datasetPath)
        result = listDatasetFiles(apiRef = localAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), nFiles)
        self.assertEqual(len(affectedBlocks), 2)
        # Create two blocks, one open, one closed, one with ten files, one with two
        if affectedBlocks[0]['OpenForWriting'] == '0':
            self.assertEqual(affectedBlocks[1]['OpenForWriting'], '1')
            self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 10)
            self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 2)
        else:
            self.assertEqual(affectedBlocks[0]['OpenForWriting'], '1')
            self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 10)
            self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 2)

        # Check parents of the child file
        # Should have twelve parents
        # All should be files we created (20 events, 1024 size)
        result = listDatasetFiles(apiRef = localAPI,
                                  datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(result), 1)
        result = localAPI.listFileParents(lfn = result[0])
        self.assertEqual(len(result), nFiles)
        for f in result:
            self.assertEqual(f['NumberOfEvents'], 20)
            self.assertEqual(f['FileSize'], 1024)


        # There should be one block in global
        # It should have ten files and be closed
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['OpenForWriting'], '0')
        self.assertEqual(result[0]['NumberOfFiles'], 10)


        # Now see what's in local

        # First grab blocks
        # Two from primary, 
        blockAction = self.bufferFactory(classname = "GetBlockFromDataset")
        names = blockAction.execute(dataset = datasetPath)
        self.assertEqual(len(names), 2)

        # One from secondary
        names = blockAction.execute(dataset = '/%s/%s_2/%s' % (name, name, tier))


        # The clumsy way
        # Should have two open blocks (one child, one primary), and one migrated block
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('Open',), ('Open',)])

        result = myThread.dbi.processData("SELECT path FROM dbsbuffer_dataset")[0].fetchall()
        self.assertEqual(len(result), 2)


        time.sleep(30)


        myThread.workerThreadManager.terminateWorkers()

        time.sleep(5)

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()

        print "Have block results"
        print myThread.dbi.processData("SELECT * FROM dbsbuffer_block")[0].fetchall()

        for entry in result:
            self.assertEqual(entry[0], 'InGlobalDBS')


        # There should be three block in global
        # All should be closed
        # One should have 10, one should have 2, and one should have 1 file
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 2)
        for block in result:
            self.assertEqual(block['OpenForWriting'], '0')
            self.assertTrue(result[0]['NumberOfFiles'] in [ 2, 10])
        result    = listBlocks(apiRef = globeAPI, datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['OpenForWriting'], '0')
        self.assertEqual(result[0]['NumberOfFiles'], 1)



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

        #return

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



    def testC_WorkerThreadTest(self):
        """
        _WorkerThreadTest_
        
        Test an individual processPool
        Good for timing tests
        """



        myThread = threading.currentThread()
        config = self.createConfig()
        config.DBSInterface.DBSBlockMaxTime = 500

        name = "ThisIsATest_%s" % (makeUUID())
        tier = "RECO"
        nFiles = 12
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = '/%s/%s/%s' % (name, name, tier)


        dbsInterface = DBSInterface(config = config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)

        # Create a config object
        from WMComponent.DBSUpload.DBSUploadPoller import createConfigForJSON
        configDict = createConfigForJSON(config)

        # Open up the buffer wrapper
        factory = WMFactory("dbsUpload",
                            "WMComponent.DBSUpload.Database.Interface")
        uploadToDBS = factory.loadObject("UploadToDBS")

        input = uploadToDBS.findUploadableDAS()
        self.assertEqual(len(input), 1)
        for das in input:
            self.assertEqual(das['AlgoInDBS'], 0)
            self.assertEqual(das['ApplicationVersion'], 'CMSSW_3_1_1')
        
        worker = DBSUploadWorker(**configDict)
        #worker(parameters = input)
        cProfile.runctx("worker(parameters = input)", globals(), locals(), filename = "testStats.stat")


        # Now see what's in DBSBuffer

        # First grab blocks
        # Two from primary, 
        blockAction = self.bufferFactory(classname = "GetBlockFromDataset")
        names = blockAction.execute(dataset = datasetPath)
        self.assertEqual(len(names), 2)

        # Shouldn't have gotten the children yet
        names = blockAction.execute(dataset = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(names), 0)


        # Now run it again
        # This is necessary to get parents
        input2 = uploadToDBS.findUploadableDAS()
        worker(parameters = input2)

        # One from secondary
        names = blockAction.execute(dataset = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(names), 1)

        # Check block status
        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('Open',), ('Open',)])



        # Now look in DBS

        # Check to see if datasets and algos are in local DBS
        result  = listAlgorithms(apiRef = localAPI, patternExe = name)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ExecutableName'], name)
        result  = listPrimaryDatasets(apiRef = localAPI, match = name)
        self.assertEqual(result, [name])
        result    = listProcessedDatasets(apiRef = localAPI, primary = name, dataTier = "*")
        #self.assertEqual(result, [name, '%s_2' % (name)])


        # Check that files are in local DBS
        affectedBlocks = listBlocks(apiRef = localAPI, datasetPath = datasetPath)
        result = listDatasetFiles(apiRef = localAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), nFiles)
        self.assertEqual(len(affectedBlocks), 2)
        # Create two blocks, one open, one closed, one with ten files, one with two
        if affectedBlocks[0]['OpenForWriting'] == '0':
            self.assertEqual(affectedBlocks[1]['OpenForWriting'], '1')
            self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 10)
            self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 2)
        else:
            self.assertEqual(affectedBlocks[0]['OpenForWriting'], '1')
            self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 10)
            self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 2)

        # Check parents of the child file
        # Should have twelve parents
        # All should be files we created (20 events, 1024 size)
        result = listDatasetFiles(apiRef = localAPI,
                                  datasetPath = '/%s/%s_2/%s' % (name, name, tier))
        self.assertEqual(len(result), 1)
        result = localAPI.listFileParents(lfn = result[0])
        self.assertEqual(len(result), nFiles)
        for f in result:
            self.assertEqual(f['NumberOfEvents'], 20)
            self.assertEqual(f['FileSize'], 1024)


        # Now check GLOBAL
        # There should be one block in global
        # It should have ten files and be closed
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['OpenForWriting'], '0')
        self.assertEqual(result[0]['NumberOfFiles'], 10)


        # Run one more time, this should do nothing
        worker(parameters = input)

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(result, [('InGlobalDBS',), ('Open',), ('Open',)])
        

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()
        

if __name__ == '__main__':
    unittest.main()
