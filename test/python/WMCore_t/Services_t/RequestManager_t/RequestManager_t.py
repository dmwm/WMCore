import os
import unittest
import tempfile
import shutil

from nose.plugins.attrib import attr

from WMCore.Services.Requests import JSONRequests
from WMCore.Services.RequestManager.RequestManager import RequestManager \
     as RequestManagerDS

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.WMSpec.StdSpecs.ReReco import getTestArguments

from WMCore_t.RequestManager_t.ReqMgr_t import getRequestSchema, RequestManagerConfig

    
class RequestManagerTest(RESTBaseUnitTest):
    """
    Test RequestMgr Service client
    It will start RequestMgr RESTService
    Server DB is whatever env is set     
    
    This checks whether DS call makes without error and return the results.
    This test only test service call returns without error.
    The correctness of each function is tested in test/python/RequestManager_t/RequestMgr_t.py
    """
    def initialize(self):
        self.couchDBName = "reqmgr_t_0"
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
        dbUrl = os.environ.get("DATABASE", None)
        self.config.setDBUrl(dbUrl)        
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()
        self.config.setupCouchDatabase(dbName = self.couchDBName)
        self.config.setPort(8888)
        self.schemaModules = ["WMCore.RequestManager.RequestDB"]
        return
        
    def setUp(self):
        """
        setUP global values
        """
        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch("%s" % self.couchDBName,
                                 "GroupUser", "ConfigCache")
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        self.reqService = RequestManagerDS(self.params)
        self.jsonSender = JSONRequests(self.config.getServerUrl())
        self.requestSchema = getRequestSchema()
        self.jsonSender.put('group/PeopleLikeMe')
        self.jsonSender.put('user/me?email=me@my.com')
        self.jsonSender.put('group/PeopleLikeMe/me')
        self.jsonSender.put('version/CMSSW_3_5_8')
        r = self.jsonSender.put('request/' + self.requestSchema['RequestName'], 
                                self.requestSchema)
        self.requestName = r[0]['RequestName']
    
    def tearDown(self):
        self.config.deleteWorkloadCache()
        RESTBaseUnitTest.tearDown(self)
        self.testInit.tearDownCouch()

    @attr("integration")
    def testA_RequestManagerService(self):
        requestName = self.requestName
        
        request = self.reqService.getRequest(requestName)
        # minimal test : it's return type  and the some value inside
        self.assertEqual(type(request), dict)
        self.assertTrue(len(request) > 0)
        
        # Test putTeam
        self.reqService.putTeam("team_usa")
        self.assertTrue('team_usa' in self.jsonSender.get('team')[0])
        
        self.jsonSender.put('assignment/%s/%s' % ("team_usa", requestName))
        
        request = self.reqService.getAssignment(teamName = "team_usa")
        self.assertEqual(type(request), list)
        self.assertTrue(len(request) > 0)
       
        request = self.reqService.getAssignment(request = requestName)
        self.assertEqual(type(request), list)
        self.assertTrue(len(request) > 0)
        
        self.reqService.sendMessage(requestName,"error")
        self.reqService.putWorkQueue(requestName, "http://test_url")
        self.reqService.reportRequestProgress(requestName)
        #self.reqService.reportRequestProgress(requestName, 
        #                percent_complete = 100, percent_success = 90)
        
        self.reqService.reportRequestStatus(requestName, "running")
        return

        
        
if __name__ == '__main__':

    unittest.main()

   
    
