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

from nose.plugins.attrib import attr

from WMCore.WMFactory       import WMFactory
from WMQuality.TestInit     import TestInit
from WMCore.DAOFactory      import DAOFactory
from WMCore.Services.UUID   import makeUUID
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID   import makeUUID

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.HeartbeatAPI  import HeartbeatAPI

from WMComponent.DBS3Buffer.DBSBufferFile   import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferUtil   import DBSBufferUtil

class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module

    """
    _maxMessage = 10


    def setUp(self):
        """
        _setUp_

        setUp function for unittest

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase = True)
        self.testInit.setSchema(customModules = ["WMComponent.DBS3Buffer"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        locationAction = self.bufferFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet")



        return



    def tearDown(self):
        """
        _tearDown_

        tearDown function for unittest
        """
        self.testInit.clearDatabase()

    def getConfig(self):
        """
        _getConfig_

        This creates the actual config file used by the component.
        """
        config = Configuration()

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        config.section_("Agent")
        config.Agent.componentName = 'DBSUpload'
        config.Agent.useHeartbeat  = False

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        config.component_("DBSUpload")
        config.DBSUpload.pollInterval     = 10
        config.DBSUpload.logLevel         = 'DEBUG'
        config.DBSUpload.DBSBlockMaxFiles = 1
        config.DBSUpload.DBSBlockMaxTime  = 2
        config.DBSUpload.DBSBlockMaxSize  = 999999999999
        #config.DBSUpload.dbsUrl           = "https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSWriter"
        #config.DBSUpload.dbsUrl           = "https://dbs3-dev01.cern.ch/dbs/prod/global/DBSWriter"
        config.DBSUpload.dbsUrl           = "https://localhost:1443/dbs/dev/global/DBSWriter"
        config.DBSUpload.namespace        = 'WMComponent.DBS3Buffer.DBSUpload'
        config.DBSUpload.componentDir     = os.path.join(os.getcwd(), 'Components')
        config.DBSUpload.nProcesses       = 1
        config.DBSUpload.dbsWaitTime      = 0.1
        return config

    def getFiles(self, name, tier, nFiles = 12, site = "malpaquet", nLumis = 1):
        """
        _getFiles_
        
        Create some dummy test files.
        """
        files = []

        (acqEra, procVer) = name.split("-")
        baseLFN = "/store/data/%s/Cosmics/RECO/%s/000/143/316/" % (acqEra, procVer)
        for f in range(nFiles):
            testFile = DBSBufferFile(lfn = baseLFN + makeUUID() + ".root", size = 1024,
                                     events = 20, checksums = {"cksum": 1})
            testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-%s/RECO" % (acqEra, procVer))
            lumis = []
            for i in range(nLumis):
                lumis.append((f * 1000000) + i)
            testFile.addRun(Run( 1, *lumis))
            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("0")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation(site)
            files.append(testFile)

        baseLFN = "/store/data/%s/Cosmics/RAW-RECO/%s/000/143/316/" % (acqEra, procVer)
        testFileChild = DBSBufferFile(lfn = baseLFN + makeUUID() + ".root",
                                      size = 1024, events = 10, checksums = {'cksum': 1})
        testFileChild.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                              appFam = "RAW-RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFileChild.setDatasetPath("/Cosmics/%s-%s/RAW-RECO" %(acqEra, procVer))
        testFileChild.addRun(Run( 1, *[45]))
        testFileChild.create()
        testFileChild.setLocation(site)

        testFileChild.addParents([x['lfn'] for x in files])
        return files

    @attr('integration')
    def testA_basicFunction(self):
        """
        _basicFunction_

        See if I can make the damn thing work.
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        from WMComponent.DBS3Buffer.DBSUploadPoller import DBSUploadPoller
        dbsUploader = DBSUploadPoller(config = config)
        dbsUtil     = DBSBufferUtil()
        from dbs.apis.dbsClient import DbsApi
        dbsApi      = DbsApi(url = config.DBSUpload.dbsUrl)

        # This should do nothing
        # Just making sure we don't crash
        try:
            dbsUploader.algorithm()
        except:
            dbsUploader.close()
            raise

        name = "ThisIsATest%s" % (int(time.time()))
        tier = "RECO"
        nFiles = 12
        name = name.replace('-', '_')
        name = '%s-v0' % name
        files = self.getFiles(name = name, tier = tier, nFiles = nFiles)
        datasetPath = "/Cosmics/%s/%s" % (name, tier)

        try:
            dbsUploader.algorithm()
        except:
            dbsUploader.close()
            raise

        time.sleep(5)

        # Now look in DBS
        try:
            result = dbsApi.listDatasets(dataset = datasetPath, detail = True,
                                         dataset_access_type = 'PRODUCTION')
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['data_tier_name'], 'RECO')
            self.assertEqual(result[0]['processing_version'], 0)
            self.assertEqual(result[0]['acquisition_era_name'], name.split('-')[0])

            result = dbsApi.listFiles(dataset=datasetPath)
            self.assertEqual(len(result), 11)
        except:
            dbsUploader.close()
            raise

        # All the blocks except for the last one should
        # now be there
        result = myThread.dbi.processData("SELECT id FROM dbsbuffer_block")[0].fetchall()
        self.assertEqual(len(result), 12)

        # The last block should still be open
        self.assertEqual(len(dbsUtil.findOpenBlocks()), 1)

        try:
            dbsUploader.algorithm()
        except:
            raise
        finally:
            dbsUploader.close()

        # All files should now be available
        result = dbsApi.listFiles(dataset=datasetPath)
        self.assertEqual(len(result), 12)


        # The last block should now be closed
        self.assertEqual(len(dbsUtil.findOpenBlocks()), 0)

        result = myThread.dbi.processData("SELECT status FROM dbsbuffer_block")[0].fetchall()
        for res in result:
            self.assertEqual(res.values()[0], 'InDBS')

        return

if __name__ == '__main__':
    unittest.main()
