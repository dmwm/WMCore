import os
import unittest
import logging
from nose.plugins.attrib import attr

from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.RequestManager.RequestManager import RequestManager as RequestManagerDS

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest

from WMCore_t.RequestManager_t.ReqMgr_t import RequestManagerConfig
from WMCore_t.RequestManager_t import utils


    
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
                
        
    def setUp(self):
        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch("%s" % self.couchDBName, "GroupUser", "ConfigCache", "ReqMgr")
        self.testInit.setupCouch("%s_wmstats" % self.couchDBName, "WMStats")
        # logging stuff from TestInit is broken, setting myself
        l = logging.getLogger()
        l.setLevel(logging.DEBUG)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        self.reqService = RequestManagerDS(self.params)
        self.jsonSender = JSONRequests(self.config.getServerUrl())

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema = utils.getAndSetupSchema(self,
                                         userName = userName,
                                         groupName = groupName,
                                         teamName = teamName)
        schema['ConfigCacheID'] = self.createConfig()
        schema['CouchDBName'] = self.couchDBName
        try:
            r = self.jsonSender.put('request/' + schema['RequestName'], schema)
            self.requestName = r[0]['RequestName']
        except Exception as ex:
            print "Exception during set up, reason: %s" % ex
            raise ex

    def tearDown(self):
        self.config.deleteWorkloadCache()
        RESTBaseUnitTest.tearDown(self)
        self.testInit.tearDownCouch()

    def createConfig(self, bad = False):
        """
        _createConfig_

        Create a config of some sort that we can load out of ConfigCache
        """
        PSetTweak = {'process': {'outputModules_': ['ThisIsAName'],
                                 'ThisIsAName': {'dataset': {'dataTier': 'RECO',
                                                             'filterName': 'Filter'}}}}
        BadTweak  = {'process': {'outputModules_': ['ThisIsAName1', 'ThisIsAName2'],
                                 'ThisIsAName1': {'dataset': {'dataTier': 'RECO',
                                                             'filterName': 'Filter'}},
                                 'ThisIsAName2': {'dataset': {'dataTier': 'RECO',
                                                             'filterName': 'Filter'}}}}
        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = self.couchDBName)
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        if bad:
            configCache.setPSetTweaks(PSetTweak = BadTweak)
        else:
            configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.save()
        return configCache.getCouchID()

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
        self.reqService.reportRequestProgress(requestName,
                        percent_complete = 100, percent_success = 90)
        
        self.reqService.reportRequestStatus(requestName, "running-open")

        
        
if __name__ == '__main__':
    unittest.main()