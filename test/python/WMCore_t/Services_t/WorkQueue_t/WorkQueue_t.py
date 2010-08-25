#!/usr/bin/env python
#setup emulator for test, this needs to be at top of the file
from WMQuality.Emulators.EmulatorSetup import emulatorSetup, deleteConfig
ConfigFile = emulatorSetup(phedex=True, dbs=True, siteDB=True, requestMgr=True)

import os
import unittest
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json


from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator


class WorkQueueTest(RESTBaseUnitTest):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB is SQlite.
    Client DB sets from environment variable. 
    """
    def initialize(self):
        self.config = DefaultConfig('WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel')
        dbUrl = os.environ.get("DATABASE", None) or "sqlite:////tmp/resttest.db"
        self.config.setDBUrl(dbUrl)        
        self.config.setFormatter('WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter')

        # mysql example
        #self.config.setDBUrl('mysql://username@host.fnal.gov:3306/TestDB')
        #self.config.setDBSocket('/var/lib/mysql/mysql.sock')
        self.schemaModules = ["WMCore.WorkQueue.Database"]
        wqConfig = self.config.getModelConfig()
        wqConfig.queueParams = {}
        wqConfig.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService']
        
    def setUp(self):
        """
        setUP global values
        """
        RESTBaseUnitTest.setUp(self)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        
        #cache location is set under current directory - Global
        self.globalQueue = globalQueue(dbi = self.testInit.getDBInterface(),
                                          CacheDir = 'Global',
                                          NegotiationTimeout = 0,
                                          QueueURL = self.config.getServerUrl())
        # original location of wmspec: under current directory - WMSpecs
        self.specGenerator = WMSpecGenerator("WMSpecs")
        
    def tearDown(self):
        RESTBaseUnitTest.tearDown(self)
        self.specGenerator.removeSpecs()
        
    def testGetWork(self):
        
        specName = "ProductionSpec"
        self.globalQueue.queueWork(self.specGenerator.createProductionSpec(specName, "file"))
        
        wqApi = WorkQueueDS(self.params)

        data = wqApi.getWork({'SiteB' : 15, 'SiteA' : 15}, "http://test.url")
        self.assertEqual( len(data) ,  1, "only 1 element needs to be back. Got (%s)" % len(data) )
        self.assertEqual(data[0]['wmspec_name'], specName)
        
    def testSynchronize(self):
        self.globalQueue.queueWork(self.specGenerator.createProcessingSpec('ProcessingSpec', "file"))
        wqApi = WorkQueueDS(self.params)
        childUrl = "http://test.url"
        childResources = [{'ParentQueueId' : 1, 'Status' : 'Available'}]
        print wqApi.synchronize(childUrl, childResources)
        
        childUrl = "http://test.url"
        childResources = []
        #print wqApi.synchronize(childUrl, childResources)
        
    def testStatusChange(self):
        raise RuntimeError, "This test causes buildbot to timeout. see - http://vpac05.phy.vanderbilt.edu:8010/builders/Unit%20Tests%20Mysql/builds/154/steps/test/logs/stdio - MELO"
        self.globalQueue.queueWork(self.specGenerator.createProcessingSpec('ProcessingSpec', "file"))
        wqApi = WorkQueueDS(self.params)

        print wqApi.status()
        print wqApi.doneWork([1])
        print wqApi.status()
        print wqApi.failWork([1])
        print wqApi.status()
        print wqApi.cancelWork([1])
        print wqApi.status()
        
if __name__ == '__main__':

    unittest.main()
    deleteConfig(ConfigFile)
    