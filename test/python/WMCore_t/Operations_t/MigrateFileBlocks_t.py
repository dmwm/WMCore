#!/usr/bin/env python

"""

DBSUpload test TestDBSUpload module and the harness

"""

__revision__ = "$Id $"
__version__ = "$Revision: 1.3 $"


import os
import threading
import time
import unittest
import WMCore.WMInit
from WMComponent.DBSUpload.DBSUpload import DBSUpload
from WMComponent.DBSUpload.DBSUploadPoller import DBSUploadPoller
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Operations.MigrateFileBlocks import MigrateFileBlocks

from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DataStructs.Run import Run
from WMCore.Agent.Configuration import Configuration

from WMCore.Services.DBS.DBSReader import DBSReader

from DBSAPI.dbsApi import DbsApi

from subprocess import Popen, PIPE



class MigrateFileBlocksTest(unittest.TestCase):
    """
    TestCase for migratingBlocks 
    
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
                                 "WMComponent.DBSBuffer.Database"],
                                useDefault = False)
      
        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        locationAction = self.bufferFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")



    def tearDown(self):
        """
        _tearDown_
        
        tearDown function for unittest
        """
        
        self.testInit.clearDatabase()

        return


    def createConfig(self):
        """
        _createConfig_

        This creates the actual config file used by the component

        """

        config = Configuration()
        config.component_("DBSUpload")
        config.DBSUpload.logLevel = 'DEBUG'
        config.DBSUpload.maxThreads = 1
        config.DBSUpload.dbsurl = \
                                'http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet'
        config.DBSUpload.dbsversion = \
                                    'DBS_2_0_6'
        config.DBSUpload.uploadFileMax = 500
        config.DBSUpload.pollInterval = 10
        config.DBSUpload.globalDBSUrl = 'http://cmssrv49.fnal.gov:8989/DBS209/servlet/DBSServlet'
        config.DBSUpload.globalDBSVer = 'DBS_2_0_8'

        #Config variables for block sizes in DBS
        config.DBSUpload.DBSMaxSize      = 999999999
        config.DBSUpload.DBSMaxFiles     = 2
        config.DBSUpload.DBSBlockTimeout = 10000000

        return config

    def addToBuffer(self, name):
        """
        _addToBuffer_

        Add files to the DBSBuffer with a set dataset path.
        """
        testFileParentA = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, cksum = 1, locations = "malpaquet")
        testFileParentA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentA.setDatasetPath("/%s/%s/RECO" %(name, name))
        testFileParentA.addRun(Run(1, *[45]))
        
        testFileParentB = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, cksum = 2, locations = "malpaquet")
        testFileParentB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentB.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentB.addRun(Run(1, *[45]))
        
        testFileParentC = DBSBufferFile(lfn = makeUUID()+'hello', size = 1024,
                                        events = 20, cksum = 3, locations = "malpaquet")
        testFileParentC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentC.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentC.addRun(Run( 1, *[46]))
        
        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()
        
        testFile = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                 events = 10, cksum = 1, locations = "malpaquet")
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


    def testMigrationInCode(self):
        """
        Test migration by loading the files

        """

        myThread = threading.currentThread()
        
        countDAO = self.bufferFactory(classname = "CountBlocks")
        randomDataset = makeUUID()

        blockCount = countDAO.execute()
        assert blockCount == 0, \
               "Error: Blocks in buffer before test started."

        config = self.createConfig()
        config.DBSUpload.DBSMaxFiles     = 40

        poller = DBSUploadPoller(config)
        poller.setup(parameters = None)
        
        for i in range(10):
            self.addToBuffer(randomDataset)
            poller.algorithm(parameters = None)
            blockCount = countDAO.execute()

        args = { "url" : config.DBSUpload.globalDBSUrl, "level" : 'ERROR', "user" :'NORMAL', "version" : config.DBSUpload.globalDBSVer }
        dbsReader = DBSReader(url = config.DBSUpload.globalDBSUrl, level='ERROR', user='NORMAL', version=config.DBSUpload.globalDBSVer)

        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, False, 'Dataset %s already migrated!' %(randomDataset))

        datasetPath = "/%s/%s/RECO" % (randomDataset, randomDataset)
        filePath = os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMCore/Operations/MigrateFileBlocks.py')
        configLocation = os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMComponent/DBSUpload/DefaultConfig.py')

        migrator = MigrateFileBlocks(config)
        migrator.migrateDataset(datasetPath)



        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, True, 'Dataset %s could not be found!' %(randomDataset))
        processed = dbsReader.listProcessedDatasets(primary = randomDataset)
        self.assertEqual(randomDataset in processed, True, 'Could not find dataset %s' %(randomDataset))
        datasetFiles =  dbsReader.listDatasetFiles('/%s/%s/%s' %(randomDataset, randomDataset, 'RECO'))
        self.assertEqual(len(datasetFiles), 30)

        #print myThread.dbi.processData("SELECT * FROM dbsbuffer_block")[0].fetchall()



    def testBlockMigration(self):
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
        config.DBSUpload.DBSMaxFiles     = 40

        poller = DBSUploadPoller(config)
        poller.setup(parameters = None)
        
        for i in range(10):
            self.addToBuffer(randomDataset)
            poller.algorithm(parameters = None)
            blockCount = countDAO.execute()

        args = { "url" : config.DBSUpload.globalDBSUrl, "level" : 'ERROR', "user" :'NORMAL', "version" : config.DBSUpload.globalDBSVer }
        dbsReader = DBSReader(url = config.DBSUpload.globalDBSUrl, level='ERROR', user='NORMAL', version=config.DBSUpload.globalDBSVer)

        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, False, 'Dataset %s already migrated!' %(randomDataset))

        datasetPath = "/%s/%s/RECO" % (randomDataset, randomDataset)
        filePath = os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMCore/Operations/MigrateFileBlocks.py')
        configLocation = os.path.join(WMCore.WMInit.getWMBASE(), 'src/python/WMComponent/DBSUpload/DefaultConfig.py')

        command = ['python2.4', filePath, datasetPath, configLocation]

        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.wait()

        primaries = dbsReader.listPrimaryDatasets()
        self.assertEqual(randomDataset in primaries, True, 'Dataset %s could not be found!' %(randomDataset))
        processed = dbsReader.listProcessedDatasets(primary = randomDataset)
        self.assertEqual(randomDataset in processed, True, 'Could not find dataset %s' %(randomDataset))
        datasetFiles =  dbsReader.listDatasetFiles('/%s/%s/%s' %(randomDataset, randomDataset, 'RECO'))
        self.assertEqual(len(datasetFiles), 30)

        statuses =  myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        results = []
        for result in statuses:
            results.append(result.values()[0])

        #There should be one open, one closed block
        self.assertEqual('InGlobalDBS' in results, True, "Could not find closed block")
        self.assertEqual('Open' in results, True, "Could not find remaining open block")
        

        return

if __name__ == '__main__':
    unittest.main()
