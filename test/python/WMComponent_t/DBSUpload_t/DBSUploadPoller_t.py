#!/usr/bin/env python

"""

DBSUpload test TestDBSUpload module and the harness

"""

__revision__ = "$Id $"
__version__ = "$Revision: 1.6 $"
__author__ = "mnorman@fnal.gov"

import commands
import logging
import os
import threading
import time
import unittest
import random

from WMComponent.DBSUpload.DBSUpload import DBSUpload
from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
import WMComponent.DBSUpload.DBSUpload
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.UUID import makeUUID
from WMCore.WMException import WMException
from WMCore.DataStructs.Run import Run

from DBSAPI.dbsApi import DbsApi



class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module 
    
    Note:
      This fails if you use the in-memory syntax for sqlite 
      i.e. (DATABASE = sqlite://)
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10


    def setUp(self):
        """
        _setUp_
        
        setUp function for unittest

        """

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        #if (os.getenv("DIALECT").lower() != 'sqlite'):
        #    print "About to tear down"
        #    self.tearDown()
        try:
            self.testInit.setSchema(customModules = ["WMCore.ThreadPool","WMCore.MsgService","WMComponent.DBSBuffer.Database"],
                                useDefault = False)
        except WMException, e:
            self.tearDown()
            raise

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet") 

        self._teardown = False

        #myThread = threading.currentThread()


        return



    def tearDown(self):
        """
        _tearDown_
        
        tearDown function for unittest

        """

        myThread = threading.currentThread()
        
        factory2 = WMFactory("MsgService", "WMCore.MsgService")
        destroy2 = factory2.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy2.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        factory = WMFactory("Threadpool", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()

        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        myThread.transaction.commit()
        
        self._teardown = True


        return


    def createConfig(self, configAddress = os.path.join(os.path.dirname(\
                        WMComponent.DBSUpload.DBSUpload.__file__), 'DefaultConfig.py')):
        """
        _createConfig_

        This creates the actual config file used by the component

        """

                # read the default config first.
        config = loadConfigurationFile(configAddress)

        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "mnorman@fnal.gov"
        config.Agent.teamName = "DBS"
        config.Agent.agentName = "DBS Upload"

        myThread = threading.currentThread()

        config.section_("General")
        
        if not os.getenv("TESTDIR") == None:
            config.General.workDir = os.getenv("TESTDIR")
        else:
            config.General.workDir = os.getcwd()
        
        config.section_("CoreDatabase")
        if not os.getenv("DIALECT") == None:
            config.CoreDatabase.dialect = os.getenv("DIALECT")
            myThread.dialect = os.getenv('DIALECT')
        if not os.getenv("DBUSER") == None:
            config.CoreDatabase.user = os.getenv("DBUSER")
        else:
            config.CoreDatabase.user = os.getenv("USER")
        if not os.getenv("DBHOST") == None:
            config.CoreDatabase.hostname = os.getenv("DBHOST")
        else:
            config.CoreDatabase.hostname = os.getenv("HOSTNAME")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        if not os.getenv("DBNAME") == None:
            config.CoreDatabase.name = os.getenv("DBNAME")
        else:
            config.CoreDatabase.name = os.getenv("DATABASE")
        if not os.getenv("DATABASE") == None:
            if os.getenv("DATABASE") == 'sqlite://':
                raise RuntimeError,\
                    "These tests will not work using in-memory SQLITE"
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            myThread.database = os.getenv("DATABASE")

        return config

    def addToBuffer(self, name):
        """
        _addToBuffer_

        This should add files to the buffer

        """

        print ""
        print "WARNING: This only works if DBSBuffer works"
        print ""

        myThread = threading.currentThread()

        #Stolen shamelessly from Steve's DBSBufferFile_t

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
        
        testFileParentC = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                        events = 20, cksum = 3, locations = "malpaquet")
        testFileParentC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentC.setDatasetPath("/%s/%s/RECO" %(name, name))        
        testFileParentC.addRun(Run( 1, *[45]))
        
        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()
        
        testFile = DBSBufferFile(lfn = makeUUID(), size = 1024,
                                 events = 10, cksum = 1, locations = "malpaquet")
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFile.setDatasetPath("/%s/%s/RECO" %(name, name))
        testFile.addRun(Run( 1, *[45]))
        testFile.create()
        
        testFile.addParent(testFileParentA["lfn"])
        testFile.addParent(testFileParentB["lfn"])
        testFile.addParent(testFileParentC["lfn"])


        #print myThread.dbi.processData("SELECT * FROM dbsbuffer_file", {})[0].fetchall()
        

        return


    def testUploadFromSelf(self):
        """
        _testUploadFromSelf_

        This may do everything itself.  It's hard to say

        """

        myThread = threading.currentThread()

        factory     = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        dbinterface = factory.loadObject("UploadToDBS")

        name = "ThisIsATest_%s" %(makeUUID())

        config = self.createConfig()
        self.addToBuffer(name)

        datasets=dbinterface.findUploadableDatasets()

        file_ids1 = []
        for dataset in datasets:
            file_ids1.extend(dbinterface.findUploadableFiles(dataset, 1000))

        self.assertEqual(len(file_ids1), 4)

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
        self.assertEqual(len(files), 4)

        fileParents = []

        for file in files:
            fileParents.append(dbswriter.listFileParents(lfn = file['LogicalFileName']))

        #Check that the final file has three parents
        self.assertEqual(len(fileParents[3]), 3)

        result = myThread.dbi.processData("SELECT * FROM dbsbuffer_block")[0].fetchall()

        self.assertEqual(len(result), 1)

        return

    
if __name__ == '__main__':
    unittest.main()
