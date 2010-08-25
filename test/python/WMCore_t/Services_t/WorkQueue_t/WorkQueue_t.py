#!/usr/bin/env python
import unittest

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig

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
        print self.config.getServerUrl()
    
    def testGetWork(self):
        
        wqApi = WorkQueue(self.params)

        print wqApi.getWork({'SiteB' : 15, 'SiteA' : 15}, "http://test.url")
         
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
