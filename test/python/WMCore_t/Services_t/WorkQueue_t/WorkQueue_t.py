#!/usr/bin/env python
import unittest
import json

from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WorkQueuePopulator import createProductionSpec, createProcessingSpec, getGlobalQueue
#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.Services.Requests import JSONThunker

class WorkQueueServiceTest(RESTBaseUnitTest):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB is SQlite.
    Client DB sets from environment variable. 
    """
    def initialize(self):
        self.config = DefaultConfig('WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel')
        self.config.setDBUrl('sqlite:////home/sryu/resttest.db')
        self.schemaModules = ["WMCore.WorkQueue.Database"]
        
        
    def setUp(self):
        """
        setUP global values
        """
        RESTBaseUnitTest.setUp(self)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        
        self.globalQueue = getGlobalQueue(dbi = self.testInit.getDBInterface(),
                                          CacheDir = 'global',
                                          NegotiationTimeout = 0,
                                          QueueURL = self.config.getServerUrl())    
        
    def testGetWork(self):
        self.globalQueue.queueWork(createProductionSpec())
        
        wqApi = WorkQueue(self.params)

        data = wqApi.getWork({'SiteB' : 15, 'SiteA' : 15}, "http://test.url")
        data = json.loads(data)
        assert len(data) == 1, "only 1 element needs to be back. Got (%s)" % len(data['data'])
        assert data[0]['wmspec_name'] == 'BasicProduction', "spec name is not BasicProduction: %s" \
                                % data['wmspec_name']
         
    def testSynchronize(self):
        wqApi = WorkQueue(self.params)
        childUrl = "http://test.url"
        childResources = [{'ParentQueueId' : 1, 'Status' : 'Available'}]
        print wqApi.synchronize(childUrl, childResources)
        
        childUrl = "http://test.url"
        childResources = []
        print wqApi.synchronize(childUrl, childResources)
        
    def testStatusChange(self):
        
        wqApi = WorkQueue(self.params)

        print wqApi.gotWork([1])
        print wqApi.status()
        print wqApi.doneWork([1])
        print wqApi.status()
        print wqApi.failWork([1])
        print wqApi.status()
        print wqApi.cancelWork([1])
        print wqApi.status()
        
if __name__ == '__main__':

    unittest.main()
