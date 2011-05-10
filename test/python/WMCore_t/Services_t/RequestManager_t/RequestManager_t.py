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

def getRequestSchema():
    schema = getTestArguments()
    schema.update(RequestName = "TestReReco", 
                  RequestType = "ReReco",
                  CmsPath = "/uscmst1/prod/sw/cms",
                  CouchURL = None,
                  CouchDBName = None,
                  Group = "PeopleLikeMe",
                  InputDataset = '/PRIM/PROC/TIER',
                  Requestor = "me"
                  )
    return schema

class RequestManagerConfig(DefaultConfig):
        
    def _setReqMgrHost(self):
        self.UnitTests.views.active.rest.model.reqMgrHost = \
              self.getServerUrl().strip('rest/')
    
    def _setWorkloadCache(self):
        self.UnitTests.views.active.rest.model.workloadCache = \
              tempfile.mkdtemp()
    
    def _setupCouchUrl(self):
        self.UnitTests.views.active.rest.couchUrl = os.environ.get("COUCHURL",None)
        
    def deleteWorkloadCache(self):
        shutil.rmtree(self.UnitTests.views.active.rest.model.workloadCache)
    
    def setupRequestConfig(self):
        import WMCore.RequestManager.RequestMaker.Processing.RecoRequest
        self.UnitTests.views.active.rest.workloadDBName = "test"
        self.UnitTests.views.active.rest.security_roles = []
        self._setReqMgrHost()
        self._setWorkloadCache()
        self._setupCouchUrl()
    
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
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
        dbUrl = os.environ.get("DATABASE", None)
        self.config.setDBUrl(dbUrl)        
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()
        # mysql example
        #self.config.setDBUrl('mysql://username@host.fnal.gov:3306/TestDB')
        #self.config.setDBSocket('/var/lib/mysql/mysql.sock')
        self.schemaModules = ["WMCore.RequestManager.RequestDB"]
        
    def setUp(self):
        """
        setUP global values
        """
        RESTBaseUnitTest.setUp(self)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        self.reqService = RequestManagerDS(self.params)
        self.jsonSender = JSONRequests(self.config.getServerUrl())
        self.requestSchema = getRequestSchema()
        self.jsonSender.put('group/PeopleLikeMe')
        self.jsonSender.put('user/me?email=me@my.com')
        self.jsonSender.put('group/PeopleLikeMe/me')
        self.jsonSender.put('version/CMSSW_3_5_8')
        self.jsonSender.put('request/' + self.requestSchema['RequestName'], 
                            self.requestSchema)
    
    def tearDown(self):
        self.config.deleteWorkloadCache()
        RESTBaseUnitTest.tearDown(self)

    @attr("integration")
    def testRequestManagerService(self):
        requestName = self.requestSchema['RequestName']
        
        request = self.reqService.getRequest('TestReReco')
        # minimal test : it's return type  and the some value inside
        self.assertEqual(type(request), dict)
        self.assertTrue(len(request) > 0)
        
        # Test putTeam
        self.reqService.putTeam("team_usa")
        self.assertTrue('team_usa' in self.jsonSender.get('team')[0])
        
        self.jsonSender.put('assignment/%s/%s' % ("team_usa", requestName))
        
        request = self.reqService.getAssignment(teamName = "team_usa")
        self.assertEqual(type(request), dict)
        self.assertTrue(len(request) > 0)
       
        #TODO: not sure why this fails. Rick? could you look at this
        request = self.reqService.getAssignment(request = requestName)
        self.assertEqual(type(request), list)
        self.assertTrue(len(request) > 0)
        
        self.reqService.sendMessage(requestName,"error")
        self.reqService.putWorkQueue(requestName, "http://test_url")
        self.reqService.reportRequestProgress(requestName, 
                        percent_complete = 100, percent_success = 90)
        
        self.reqService.reportRequestStatus(requestName, "running")
        
        
if __name__ == '__main__':

    unittest.main()

   
    
