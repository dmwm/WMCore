#!/usr/bin/env python
#pylint: disable-msg=E1101, W6501, W0142, C0103, W0401
# W0401: I am not going to import all those functions by hand

"""

Test for the DBSInterface

"""

__revision__ = "$Id: DBSInterface_t.py,v 1.2 2010/05/21 21:32:33 mnorman Exp $"
__version__ = "$Revision: 1.2 $"


import os
import threading
import time
import unittest
import re

from WMCore.Services.UUID       import makeUUID
from WMQuality.TestInit         import TestInit
from WMCore.Agent.Configuration import Configuration

from WMCore.DataStructs.Run   import Run


from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile



from WMComponent.DBSUpload.DBSInterface import *



class DBSInterfaceTest(unittest.TestCase):
    """
    Test module for the DBSInterface

    """



    def setUp(self):
        """
        Basic setUp

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        return


    def tearDown(self):
        """
        Basic tearDown


        """


        return


    def createConfig(self):
        """
        Create a config object for testing

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
            testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/%s/%s/%s" % (name, name, tier))
            testFile.addRun(Run( 1, *[f]))
            testFile['locations'].add(site)
            files.append(testFile)


        return files


    def createAlgoDataset(self, name, datasetPath):
        """
        Create DBSUpload style algo and dataset info
        
        """

        algo = {'ApplicationName':    name,
                'ApplicationFamily':  'RECO',
                'ApplicationVersion': 'CMSSW_3_1_1',
                'PSetHash':           'GIBBERISH',
                'PSetContent':        'MOREGIBBERISH',
                'InDBS':              0
                }

        dataset = {'ID':               1,
                   'Path':             datasetPath,
                   'ProcessedDataset': name,
                   'PrimaryDataset':   name,
                   'DataTier':         'RECO',
                   'Algo':             None,
                   'AlgoInDBS':        None
                   }

        return algo, dataset


    def testA_directReadWrite(self):
        """
        Test whether you can read and write directly into DBS using DBSInterface

        """

        config = self.createConfig()

        name   = "ThisIsATest_%s" %(makeUUID())
        tier   = "RECO"
        nFiles = 12
        datasetPath = '/%s/%s/%s' % (name, name, tier)
        files  = self.getFiles(name = name, tier = tier, nFiles = nFiles)
		

        dbsInterface = DBSInterface(config)
        localAPI = dbsInterface.getAPIRef()



        # Can we create an algo?
        algo = createAlgorithm(apiRef = localAPI, appName = name,
                               appVer = "CMSSW_3_1_1", appFam = "RECO")
        result  = listAlgorithms(apiRef = localAPI, patternExe = name)
        self.assertEqual(len(result), 1)  # Should only be one algo
        self.assertEqual(result[0]['ExecutableName'], name)
        

        # Can we create a primary dataset?
        primary = createPrimaryDataset(primaryName = name, apiRef = localAPI)
        result  = listPrimaryDatasets(apiRef = localAPI, match = name)
        self.assertEqual(result, [name])


        # Can we create a processed dataset?
        processed = createProcessedDataset(algorithm = algo, apiRef = localAPI,
                                           primary = primary, processedName = name,
                                           dataTier = tier)
        result    = listProcessedDatasets(apiRef = localAPI, primary = name, dataTier = "*")
        self.assertEqual(result, [name])


        # Can we create file blocks?
        fileBlock = createFileBlock(apiRef = localAPI, datasetPath = datasetPath, seName = 'test')
        result    = listBlocks(apiRef = localAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)  # There should be only one result
        self.assertEqual(result[0]['Path'], datasetPath)
        self.assertEqual(fileBlock['newFiles'], [])
        self.assertEqual(fileBlock['NumberOfFiles'], 0)
        self.assertEqual(fileBlock['OpenForWriting'], '1')


        # Can we create files?
        dbsfiles = []
        for bfile in files:
            for run in bfile.getRuns():
                insertDBSRunsFromRun(apiRef = localAPI, dSRun = run)
            dbsfiles.append(createDBSFileFromBufferFile(procDataset = processed, bufferFile = bfile))
        #print dbsfiles
        insertFiles(apiRef = localAPI, datasetPath = datasetPath, files = dbsfiles, block = fileBlock)
        result = listDatasetFiles(apiRef = localAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), nFiles)
        for filename in result:
            self.assertTrue(re.search(name, filename))


        # Can we close blocks?
        closeBlock(apiRef = localAPI, block = fileBlock)
        result    = listBlocks(apiRef = localAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1) # Only got one block back
        self.assertEqual(result[0]['OpenForWriting'], '0')



        return



    def testB_DBSInterfaceSimple(self):
        """
        _DBSInterfaceSimple_

        Do some simple checks using the DBSInterface methods instead
        of the individual functions.
        """



        config = self.createConfig()

        name   = "ThisIsATest_%s" %(makeUUID())
        tier   = "RECO"
        nFiles = 12
        datasetPath = '/%s/%s/%s' % (name, name, tier)
        files  = self.getFiles(name = name, tier = tier, nFiles = nFiles)


        dbsInterface = DBSInterface(config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)

        algo, dataset = self.createAlgoDataset(name = name, datasetPath = datasetPath)



        # Can we insert a dataset algo?
        affectedBlocks = dbsInterface.runDBSBuffer(algo = algo, dataset = dataset,
                                                   files = files)
        result  = listAlgorithms(apiRef = localAPI, patternExe = name)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ExecutableName'], name)
        result  = listPrimaryDatasets(apiRef = localAPI, match = name)
        self.assertEqual(result, [name])
        result    = listProcessedDatasets(apiRef = localAPI, primary = name, dataTier = "*")
        self.assertEqual(result, [name])


        result = listDatasetFiles(apiRef = localAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 12)
        self.assertEqual(len(affectedBlocks), 2)
        # Create two blocks, one open, one closed, one with ten files, one with two
        self.assertEqual(affectedBlocks[0]['OpenForWriting'], '0')
        self.assertEqual(affectedBlocks[1]['OpenForWriting'], '1')
        self.assertEqual(affectedBlocks[0]['NumberOfFiles'], 10)
        self.assertEqual(affectedBlocks[1]['NumberOfFiles'], 2)


        # There should be one block in global
        # It should have ten files and be closed
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['OpenForWriting'], '0')
        self.assertEqual(result[0]['NumberOfFiles'], 10)


        return




    def testC_MultipleSites(self):
        """
        _MultipleSites_

        See if it opens blocks in multiple sites.
        """


        files = []

        config = self.createConfig()

        name   = "ThisIsATest_%s" %(makeUUID())
        tier   = "RECO"
        nFiles = 12
        datasetPath = '/%s/%s/%s' % (name, name, tier)
        files.extend(self.getFiles(name = name, tier = tier, nFiles = nFiles, site = 'Ramilles'))
        files.extend(self.getFiles(name = name, tier = tier, nFiles = nFiles, site = 'Blenheim'))


        dbsInterface = DBSInterface(config)
        localAPI     = dbsInterface.getAPIRef()
        globeAPI     = dbsInterface.getAPIRef(globalRef = True)

        algo, dataset = self.createAlgoDataset(name = name, datasetPath = datasetPath)

        affectedBlocks = dbsInterface.runDBSBuffer(algo = algo, dataset = dataset,
                                                   files = files)
        result  = listAlgorithms(apiRef = localAPI, patternExe = name)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ExecutableName'], name)
        result  = listPrimaryDatasets(apiRef = localAPI, match = name)
        self.assertEqual(result, [name])
        result    = listProcessedDatasets(apiRef = localAPI, primary = name, dataTier = "*")
        self.assertEqual(result, [name])


        # Should have four blocks, two for each site
        self.assertEqual(len(affectedBlocks), 4)
        RamBlocks = []
        BleBlocks = []
        for block in affectedBlocks:
            if block['StorageElementList'][0]['Name'] == 'Blenheim':
                BleBlocks.append(block)
            elif block['StorageElementList'][0]['Name'] == 'Ramilles':
                RamBlocks.append(block)

        self.assertEqual(len(RamBlocks), 2)
        self.assertEqual(len(BleBlocks), 2)

        self.assertEqual(RamBlocks[0]['NumberOfFiles'], 10)
        self.assertEqual(BleBlocks[0]['NumberOfFiles'], 10)
        self.assertEqual(RamBlocks[1]['NumberOfFiles'], 2)
        self.assertEqual(BleBlocks[1]['NumberOfFiles'], 2)


        # We should have two blocks in global
        # Both should have ten files, and both should be closed
        result    = listBlocks(apiRef = globeAPI, datasetPath = datasetPath)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['OpenForWriting'], '0')
        self.assertEqual(result[0]['NumberOfFiles'], 10)


        return

        
        

if __name__ == '__main__':
    unittest.main()
