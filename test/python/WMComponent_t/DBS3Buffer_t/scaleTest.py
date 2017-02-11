#!/usr/bin/env python

"""
_scaleTest_

Fills a DBSBuffer with random data and then runs a DBSUpload process on it.
"""
from __future__ import print_function
import os
import sys
import time
import random
import logging
import threading

from WMCore.Services.UUIDLib       import makeUUID
from WMQuality.TestInit         import TestInit
from WMQuality.Emulators        import EmulatorSetup
from WMCore.Agent.Configuration import Configuration
from WMCore.DAOFactory          import DAOFactory
from WMCore.DataStructs.Run     import Run

from WMComponent.DBS3Buffer.DBSBufferFile   import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferUtil   import DBSBufferUtil
from WMComponent.DBS3Buffer.DBSUploadPoller import DBSUploadPoller

class scaleTestFiller:
    """
    _scaleTestFiller_

    Initializes the DB and the DBSUploader
    On __call__() it creates data and uploads it.
    """

    def __init__(self):
        """
        __init__

        Init the DB
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase = True)
        self.testInit.setSchema(customModules = ["WMComponent.DBS3Buffer"],
                                useDefault = False)
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        locationAction = self.bufferFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet")

        config = self.getConfig()
        self.dbsUploader = DBSUploadPoller(config = config)

        return


    def __call__(self):
        """
        __call__

        Generate some random data
        """

        # Generate somewhere between one and a thousand files
        name = "ThisIsATest_%s" % (makeUUID())
        nFiles = random.randint(10, 2000)
        name = name.replace('-', '_')
        name = '%s-v0' % name
        files = self.getFiles(name = name, nFiles = nFiles)

        print("Inserting %i files for dataset %s" % (nFiles * 2, name))

        try:
            self.dbsUploader.algorithm()
        except:
            self.dbsUploader.close()
            raise

        # Repeat just to make sure
        try:
            self.dbsUploader.algorithm()
        except:
            self.dbsUploader.close()
            raise


        return



    def getConfig(self):
        """
        _getConfig_

        This creates the actual config file used by the component

        """


        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

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


        config.component_("DBS3Upload")
        config.DBS3Upload.pollInterval     = 10
        config.DBS3Upload.logLevel         = 'DEBUG'
        config.DBS3Upload.DBSBlockMaxFiles = 500
        config.DBS3Upload.DBSBlockMaxTime  = 600
        config.DBS3Upload.DBSBlockMaxSize  = 999999999999
        config.DBS3Upload.dbsUrl           = 'http://cms-xen40.fnal.gov:8787/dbs/prod/global/DBSWriter'
        config.DBS3Upload.namespace        = 'WMComponent.DBS3Buffer.DBS3Upload'
        config.DBS3Upload.componentDir     = os.path.join(os.getcwd(), 'Components')
        config.DBS3Upload.nProcesses       = 1
        config.DBS3Upload.dbsWaitTime      = 1

        return config



    def getFiles(self, name, tier = 'RECO', nFiles = 12, site = "malpaquet", nLumis = 1):
        """
        Create some quick dummy test files

        """

        files = []

        for f in range(nFiles):
            testFile = DBSBufferFile(lfn = '/data/store/random/random/RANDOM/test/0/%s-%s-%i.root' % (name, site, f), size = 1024,
                                     events = 20, checksums = {'cksum': 1})
            testFile.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/%s/%s/%s" % (name, name, tier))
            lumis = []
            for i in range(nLumis):
                lumis.append((f * 100000) + i)
            testFile.addRun(Run( 1, *lumis))
            testFile.setAcquisitionEra(name.split('-')[0])
            testFile.setProcessingVer("0")
            testFile.setGlobalTag("Weird")
            testFile.create()
            testFile.setLocation(site)
            files.append(testFile)

        count = 0
        for f in files:
            count += 1
            testFileChild = DBSBufferFile(lfn = '/data/store/random/random/RANDOM/test/0/%s-%s-%i-child.root' %(name, site, count), size = 1024,
                                          events = 10, checksums = {'cksum': 1})
            testFileChild.setAlgorithm(appName = name, appVer = "CMSSW_3_1_1",
                                       appFam = "RECO", psetHash = "GIBBERISH",
                                       configContent = "MOREGIBBERISH")
            testFileChild.setDatasetPath("/%s/%s_2/RECO" %(name, name))
            testFileChild.addRun(Run( 1, *[45]))
            testFileChild.create()
            testFileChild.setLocation(site)

            testFileChild.addParents([f['lfn']])


        return files



if __name__ == "__main__":
    """
    Run the code

    """

    if False: # Enable test at your own risk
        scaleTester = scaleTestFiller()
        while True:
            print("Ready to begin scale testing")
            scaleTester()
            print("Done running for now, sleeping temporarily")
            time.sleep(random.randint(10, 60))
