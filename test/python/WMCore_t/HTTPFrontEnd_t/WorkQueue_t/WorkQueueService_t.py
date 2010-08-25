#!/usr/bin/env python

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
from WMCore_t.Services_t.WorkQueue_t.WorkQueuePopulator import createProductionSpec, createProcessingSpec, getGlobalQueue
#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMCore.Wrappers import JsonWrapper

class WorkQueueServiceTest(RESTBaseUnitTest):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB is SQlite.
    Client DB sets from environment variable. 
    """
    def initialize(self):
        self.config = DefaultConfig('WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel')
        
        # set up database
        dbUrl = os.environ["DATABASE"] or "sqlite:////tmp/resttest.db"
        self.config.setDBUrl(dbUrl)
        #self.config.setDBUrl('mysql://username@host.fnal.gov:3306/TestDB')
        self.urlbase = self.config.getServerUrl()
        
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
        
        self.globalQueue = getGlobalQueue(dbi = self.testInit.getDBInterface(),
                                          CacheDir = 'global',
                                          NegotiationTimeout = 0,
                                          QueueURL = self.config.getServerUrl())    
        
    def testGetWork(self):
        self.globalQueue.queueWork(createProductionSpec())
        
        verb ='POST'
        url = self.urlbase + 'getwork/'
        input = {'siteJobs':{'SiteB' : 15, 'SiteA' : 15}, 
                 "pullingQueueUrl": "http://test.url"}
        input = JsonWrapper.dumps(input)
        contentType = "application/json"
        output={'code':200, 'type':'text/json'}
        
        data, expires = methodTest(verb, url, input=input, contentType=contentType, output=output)
        data = JsonWrapper.loads(data)
        
        assert len(data) == 1, "only 1 element needs to be back. Got (%s)" % len(data)
        assert data[0]['wmspec_name'] == 'BasicProduction', "spec name is not BasicProduction: %s" \
                                % data['wmspec_name']
         
         
if __name__ == '__main__':

    unittest.main()
