#!/usr/bin/env python

"""

DBSUpload test TestDBSUpload module and the harness

"""

__revision__ = "$Id: DBSUploadPoller_t.py,v 1.16 2009/12/10 20:37:17 mnorman Exp $"
__version__ = "$Revision: 1.16 $"


import os
import threading
import time
import unittest

from WMComponent.DBSUpload.DBSUpload import DBSUpload
from WMComponent.DBSUpload.DBSUploadPoller import DBSUploadPoller
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
import WMComponent.DBSUpload.DBSUpload
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DataStructs.Run import Run

from WMCore.Services.DBS.DBSReader import DBSReader

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
        #self.testInit.clearDatabase(modules = ["WMCore.ThreadPool", "WMCore.MsgService"])
        self.testInit.setSchema(customModules = 
                                ["WMCore.ThreadPool",
                                 "WMCore.MsgService",
                                 "WMComponent.DBSBuffer.Database"],
                                useDefault = False)
      
        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        locationAction = self.bufferFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        #locationAction.execute(siteName = "malpaquet") 


    def tearDown(self):
        """
        _tearDown_
        
        tearDown function for unittest
        """
        
        self.testInit.clearDatabase()

    def createConfig(self, configAddress = os.path.join(os.path.dirname(\
                        WMComponent.DBSUpload.DBSUpload.__file__), 'DefaultConfig.py')):
        """
        _createConfig_

        This creates the actual config file used by the component

        """

                # read the default config first.
        config = self.testInit.getConfiguration(configAddress)
        config.General.workDir = os.getcwd()
        config.DBSUpload.uploadFileMax = 150

        return config

    def addToBuffer(self, name):
        """
        _addToBuffer_

        Add files to the DBSBuffer with a set dataset path.
        """
        testFileParentA = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, checksums = {'cksum': 1}, locations = "malpaquet")
        testFileParentA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentA.setDatasetPath("/%s/%s/RECO" %(name, name))
        testFileParentA.addRun(Run(1, *[45]))
        
        testFileParentB = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, checksums = {'cksum': 1}, locations = "malpaquet")
        testFileParentB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentB.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentB.addRun(Run(1, *[45]))
        
        testFileParentC = DBSBufferFile(lfn = makeUUID()+'hello', size = 1024,
                                        events = 20, checksums = {'cksum': 1}, locations = "malpaquet")
        testFileParentC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentC.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentC.addRun(Run( 1, *[46]))
        
        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()
        
        testFile = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                 events = 10, checksums = {'cksum': 1}, locations = "malpaquet")
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFile.setDatasetPath("/%s/%s_2/RECO" %(name, name))
        testFile.addRun(Run( 1, *[45]))
        testFile.create()

        #testFile.addParents([testFileParentA["lfn"]])
        #testFile.addParents([testFileParentA["lfn"], testFileParentB["lfn"]])
        testFile.addParents([testFileParentA["lfn"], testFileParentB["lfn"], testFileParentC["lfn"]] )

        return

    def bulkAddToBuffer(self, name):
        """
        _addToBuffer_

        This should add files to the buffer

        """

        myThread = threading.currentThread()

        #Stolen shamelessly from Steve's DBSBufferFile_t

        testFiles = []

        testFileParentA = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, checksums = {'cksum': 1}, locations = "malpaquet")
        testFileParentA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentA.setDatasetPath("/%s/%s/RECO" %(name, name))
        testFileParentA.addRun(Run(1, *[45]))
        
        testFileParentB = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, checksums = {'cksum': 1}, locations = "malpaquet")
        testFileParentB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentB.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentB.addRun(Run(1, *[45]))
        
        testFileParentC = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, checksums = {'cksum': 1}, locations = "malpaquet")
        testFileParentC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentC.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentC.addRun(Run( 1, *[45]))
        
        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()

        for i in range(0,200):
                testFile = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                         events = 10, checksums = {'cksum': 1}, locations = "malpaquet")
                testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                      appFam = "RECO", psetHash = "GIBBERISH",
                                      configContent = "MOREGIBBERISH")
                testFile.setDatasetPath("/%s/%s/RECO" %(name, name))
                testFile.addRun(Run( 1, *[45]))
                testFile.create()

                testFile.addParents([testFileParentA["lfn"], testFileParentB["lfn"], testFileParentC["lfn"]] )
        
                testFiles.append(testFile)

        return


    def testUploadFromSelf(self):
        """
        _testUploadFromSelf_

        This may do everything itself.  It's hard to say

        """

        #return

        myThread = threading.currentThread()

        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        name = "ThisIsATest_%s" %(makeUUID())

        config = self.createConfig()
        config.DBSUpload.componentDir = os.path.join(os.getcwd(), 'Components')
        self.addToBuffer(name)

        datasets=dbinterface.findUploadableDatasets()

        file_ids1 = []
        for dataset in datasets:
            file_ids1.extend(dbinterface.findUploadableFiles(dataset, 1000))

        #self.assertEqual(len(file_ids1), 4)

        testDBSUpload = DBSUpload(config)
        testDBSUpload.prepareToStart()

        #self.addSecondBatch()
        myThread.workerThreadManager.terminateWorkers()
        datasets=dbinterface.findUploadableDatasets()

        file_ids = []
        file_list = []
        for dataset in datasets:
            file_ids.extend(dbinterface.findUploadableFiles(dataset, 1000))
        for id in file_ids1:
            tempFile = DBSBufferFile(id = id["ID"])
            tempFile.load(parentage = 1)
            file_list.append(tempFile)

        #self.assertEqual(len(file_ids), 0)

        #child = file_list[3]

        #self.assertEqual(len(child['parents']), 3)

        dbsurl     = config.DBSUpload.dbsurl
        dbsversion = config.DBSUpload.dbsversion

        args = { "url" : dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" : dbsversion }
        #conf = {"level" : 'ERROR', "user" :'NORMAL', "version" : dbsversion }
        dbswriter = DbsApi(args)
        #dbsreader = DBSReader(dbsurl)
        primaryDatasets   = dbswriter.listPrimaryDatasets('*')
        processedDatasets = dbswriter.listProcessedDatasets()
        dbsAlgos          = dbswriter.listAlgorithms()
        
        datasetNames   = []
        processedNames = []
        algoVer        = []
        #print primaryDatasets
        for dataset in primaryDatasets:
            datasetNames.append(dataset['Name'])

        for dataset in processedDatasets:
            processedNames.append(dataset['Name'])

        for algo in dbsAlgos:
            #print algo
            algoVer.append(algo['ApplicationVersion'])

        #Check for primary and processed dataset and application of correct version
        self.assertEqual(name in datasetNames, True)
        self.assertEqual(name in processedNames, True)
        self.assertEqual('CMSSW_3_1_1' in algoVer, True)

        datasetPath = "/%s/%s/RECO" %(name, name)

        files = dbswriter.listDatasetFiles(datasetPath = datasetPath)

        #Check that there are four files
        self.assertEqual(len(files), 4)

        fileParents = []

        for file in files:
            fileParents.append(dbswriter.listFileParents(lfn = file['LogicalFileName']))

        #Check that the final file has three parents
        self.assertEqual(len(fileParents[3]), 3)

        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_block")[0].fetchall()

        self.assertEqual(len(result), 2)

        #Is the algo listed as being in DBS?
        result = myThread.dbi.processData("SELECT in_dbs FROM dbsbuffer_algo")[0].fetchall()[0].values()[0]

        self.assertEqual(result, 1)

        result = myThread.dbi.processData("SELECT blockname FROM dbsbuffer_block WHERE id IN (SELECT block_id FROM dbsbuffer_file)")[0].fetchall()

        self.assertEqual(len(result), 2)

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()

        self.assertEqual(result[0].values()[0], 'InGlobalDBS')
        self.assertEqual(result[1].values()[0], 'InGlobalDBS')

        return


    def testLargeUpload(self):
        return
        myThread = threading.currentThread()

        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        name = "ThisIsATest_%s" %(makeUUID())

        config = self.createConfig()
        self.bulkAddToBuffer(name)

        datasets=dbinterface.findUploadableDatasets()

        file_ids1 = []
        for dataset in datasets:
            file_ids1.extend(dbinterface.findUploadableFiles(dataset, 1000))


        self.assertEqual(len(file_ids1), 203)

        testDBSUpload = DBSUpload(config)
        testDBSUpload.prepareToStart()


        time.sleep(30)

        myThread.workerThreadManager.terminateWorkers()
        datasets=dbinterface.findUploadableDatasets()

        file_ids = []
        file_list = []
        for dataset in datasets:
            file_ids.extend(dbinterface.findUploadableFiles(dataset, 1000))
        for id in file_ids1:
            tempFile = DBSBufferFile(id = id["ID"])
            tempFile.load(parentage = 1)
            file_list.append(tempFile)



        self.assertEqual(len(file_ids), 0)

        child = file_list[3]

        self.assertEqual(len(child['parents']), 3)

        dbsurl     = config.DBSUpload.dbsurl
        dbsversion = config.DBSUpload.dbsversion

        args = { "url" : dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" : dbsversion }
        #conf = {"level" : 'ERROR', "user" :'NORMAL', "version" : dbsversion }
        dbswriter = DbsApi(args)
        #dbsreader = DBSReader(dbsurl)
        primaryDatasets   = dbswriter.listPrimaryDatasets('*')
        processedDatasets = dbswriter.listProcessedDatasets()
        dbsAlgos          = dbswriter.listAlgorithms()
        
        datasetNames   = []
        processedNames = []
        algoVer        = []
        #print primaryDatasets
        for dataset in primaryDatasets:
            datasetNames.append(dataset['Name'])

        for dataset in processedDatasets:
            processedNames.append(dataset['Name'])

        for algo in dbsAlgos:
            #print algo
            algoVer.append(algo['ApplicationVersion'])

        #Check for primary and processed dataset and application of correct version
        self.assertEqual(name in datasetNames, True)
        self.assertEqual(name in processedNames, True)
        self.assertEqual('CMSSW_3_1_1' in algoVer, True)

        datasetPath = "/%s/%s/RECO" %(name, name)

        files = dbswriter.listDatasetFiles(datasetPath = datasetPath)

        #Check that there are four files
        self.assertEqual(len(files), 203)

        fileParents = []

        for file in files:
            fileParents.append(dbswriter.listFileParents(lfn = file['LogicalFileName']))

        time.sleep(10)

        #Check that the final file has three parents
        self.assertEqual(len(fileParents[3]), 3)

        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_block")[0].fetchall()

        self.assertEqual(len(result), 102)

        #Is the algo listed as being in DBS?
        result = myThread.dbi.processData("SELECT in_dbs FROM dbsbuffer_algo")[0].fetchall()[0].values()[0]

        self.assertEqual(result, 1)

        return

    def testBlockCreation(self):
        """
        _testBlockCreation_

        Run the poller several times and make sure it doesn't unnecessarily
        create blocks.
        """

        #return

        myThread = threading.currentThread()
        
        countDAO = self.bufferFactory(classname = "CountBlocks")
        randomDataset = makeUUID()

        blockCount = countDAO.execute()
        assert blockCount == 0, \
               "Error: Blocks in buffer before test started."

        config = self.createConfig()
        config.DBSUpload.DBSMaxFiles     = 2
        
        poller = DBSUploadPoller(config)
        poller.setup(parameters = None)

        for i in range(10):
            self.addToBuffer(randomDataset)
            poller.algorithm(parameters = None)
            blockCount = countDAO.execute()

            print "Have %i block" % (blockCount)

            #assert blockCount == 1, \
            #       "Error: Wrong number of blocks in buffer: %s" % blockCount


        args = { "url" : config.DBSUpload.globalDBSUrl, "level" : 'ERROR', "user" :'NORMAL', "version" : config.DBSUpload.globalDBSVer }
        dbsReader = DBSReader(url = config.DBSUpload.globalDBSUrl, level='ERROR', user='NORMAL', version=config.DBSUpload.globalDBSVer)

        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, True, 'Could not find dataset %s' %(randomDataset))
        processed = dbsReader.listProcessedDatasets(primary = randomDataset)
        self.assertEqual(randomDataset in processed, True, 'Could not find dataset %s' %(randomDataset))
        datasetFiles =  dbsReader.listDatasetFiles('/%s/%s/%s' %(randomDataset, randomDataset, 'RECO'))
        self.assertEqual(len(datasetFiles), 28)


        

        return


    def testBlockTimeout(self):
        """
        _testBlockTimeout_
        
        Test closing blocks via timeout
        """

        myThread = threading.currentThread()

        countDAO = self.bufferFactory(classname = "CountBlocks")
        randomDataset = makeUUID()

        blockCount = countDAO.execute()
        assert blockCount == 0, \
               "Error: Blocks in buffer before test started."

        config = self.createConfig()
        config.DBSUpload.DBSBlockTimeout = 20
        config.DBSUpload.DBSMaxFiles     = 40
        
        poller = DBSUploadPoller(config)
        poller.setup(parameters = None)

        for i in range(5):
            self.addToBuffer(randomDataset)
            poller.algorithm(parameters = None)
            blockCount = countDAO.execute()

        time.sleep(30)
        for i in range(5):
            self.addToBuffer(randomDataset)
            poller.algorithm(parameters = None)
            blockCount = countDAO.execute()

            #assert blockCount == 1, \
            #       "Error: Wrong number of blocks in buffer: %s" % blockCount


        args = { "url" : config.DBSUpload.globalDBSUrl, "level" : 'ERROR', "user" :'NORMAL', "version" : config.DBSUpload.globalDBSVer }
        dbsReader = DBSReader(url = config.DBSUpload.globalDBSUrl, level='ERROR', user='NORMAL', version=config.DBSUpload.globalDBSVer)

        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, True, 'Could not find dataset %s' %(randomDataset))
        processed = dbsReader.listProcessedDatasets(primary = randomDataset)
        self.assertEqual(randomDataset in processed, True, 'Could not find dataset %s' %(randomDataset))
        datasetFiles =  dbsReader.listDatasetFiles('/%s/%s/%s' %(randomDataset, randomDataset, 'RECO'))
        self.assertEqual(len(datasetFiles), 15)
        

        time.sleep(2*config.DBSUpload.DBSBlockTimeout + 1)
        poller.algorithm(parameters = None)


        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, True, 'Could not find dataset %s' %(randomDataset))
        processed = dbsReader.listProcessedDatasets(primary = randomDataset)
        self.assertEqual(randomDataset in processed, True, 'Could not find dataset %s' %(randomDataset))
        datasetFiles =  dbsReader.listDatasetFiles('/%s/%s/%s' %(randomDataset, randomDataset, 'RECO'))
        self.assertEqual(len(datasetFiles), 30)
        

        for entry in myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall():
            self.assertEqual(entry.values()[0], 'InGlobalDBS')


        return

if __name__ == '__main__':
    unittest.main()
