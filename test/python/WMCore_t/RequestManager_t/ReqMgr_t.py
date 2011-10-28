#!/usr/bin/env python

"""
RequestManager unittest


"""

from WMCore.Services.Requests import JSONRequests
import WMCore_t.RequestManager_t.FakeRequests as FakeRequests
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest as ReRecoRequest
import WMCore.WMSpec.StdSpecs.ReReco as ReReco
import unittest
from WMCore.Wrappers import JsonWrapper as json
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from httplib import HTTPException
import urllib
import tempfile
import shutil
import os
import threading
from nose.plugins.attrib import attr
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

    def setupCouchDatabase(self, dbName):
        self.UnitTests.views.active.rest.configDBName = dbName
        
class TestReqMgr(RESTBaseUnitTest):
    """
    _TestReqMgr_

    Basic test for the ReqMgr services.
    Setup is done off-screen in RESTBaseUnitTest - this makes
    things confusing
    """

    def setUp(self):
        """
        setUP global values
        Database setUp is done in base class
        """
        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch("%s" % self.couchDBName,
                                 "GroupUser", "ConfigCache")

        reqMgrHost      = self.config.getServerUrl()
        self.jsonSender = JSONRequests(reqMgrHost)
        return

    def initialize(self):
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
        self.couchDBName = "reqmgr_t_0"
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()
        self.config.setupCouchDatabase(dbName = self.couchDBName)
        self.config.setPort(8888)
        self.schemaModules = ["WMCore.RequestManager.RequestDB"]
        return

    def tearDown(self):
        """
        tearDown 

        Tear down everything
        """
        RESTBaseUnitTest.tearDown(self)
        self.testInit.tearDownCouch()
        return

    @attr("integration")
    def testA_testBasicSetUp(self):
        """
        _testBasicSetUp_
        
        Moving the tests that were in the setUp category out of it,
        mostly because I want to make sure that they don't fail inside
        the setUp statement.
        """
        if 'me' in self.jsonSender.get('user')[0]:
            self.jsonSender.delete('user/me')    
        self.assertFalse('me' in self.jsonSender.get('user')[0])
        self.assertEqual(self.jsonSender.put('user/me?email=me@my.com')[1], 200)
        self.assertTrue('me' in self.jsonSender.get('user')[0])

        if 'PeopleLikeMe' in self.jsonSender.get('group')[0]:
            self.jsonSender.delete('group/PeopleLikeMe')
        self.assertFalse('PeopleLikeMe' in self.jsonSender.get('group')[0])
        self.assertEqual(self.jsonSender.put('group/PeopleLikeMe')[1], 200)
        self.assertTrue( 'PeopleLikeMe' in self.jsonSender.get('group')[0])

        self.jsonSender.put('group/PeopleLikeMe/me')
        users = json.loads(self.jsonSender.get('group/PeopleLikeMe')[0])['users']
        self.assertTrue('me' in users)
        groups = json.loads(self.jsonSender.get('user/me')[0])['groups']
        self.assertTrue('PeopleLikeMe' in groups)
        groups2 = self.jsonSender.get('group?user=me')[0]
        self.assertTrue('PeopleLikeMe' in groups2)

        if 'White Sox' in self.jsonSender.get('team')[0]:
            self.jsonSender.delete(urllib.quote('team/White Sox'))
        self.assertFalse('White Sox' in self.jsonSender.get('team')[0])
        self.assertEqual(self.jsonSender.put(urllib.quote('team/White Sox'))[1], 200)
        self.assertTrue('White Sox' in self.jsonSender.get('team')[0])

        # some foreign key stuff to dealwith
        self.assertTrue(self.jsonSender.put('version/CMSSW_3_5_8')[1] == 200)
        self.assertTrue('CMSSW_3_5_8' in self.jsonSender.get('version')[0])
        return
        

    @attr("integration")
    def testB_ReReco(self):
        # Do the basic setup of the group/team space
        self.jsonSender.put('user/me?email=me@my.com')
        self.jsonSender.put('group/PeopleLikeMe')
        self.jsonSender.put('group/PeopleLikeMe/me')
        self.jsonSender.put(urllib.quote('team/White Sox'))
        self.jsonSender.put('version/CMSSW_3_5_8')
                            
        schema = ReReco.getTestArguments()
        schema['RequestName'] = 'TestReReco'
        schema['RequestType'] = 'ReReco'
        schema['CmsPath'] = "/uscmst1/prod/sw/cms"
        self.doRequest(schema)
        return

    def doRequest(self, schema):
        schema['CmsPath'] = "/uscmst1/prod/sw/cms"
        schema['Requestor'] = 'me'
        schema['Group'] = 'PeopleLikeMe'
        requestName = schema['RequestName']
        self.assertRaises(HTTPException, self.jsonSender.delete, 'request/%s' % requestName)
        result = self.jsonSender.put('request/%s' % (requestName), schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        self.assertEqual(self.jsonSender.get('request/%s' % requestName)[0]['RequestName'], requestName)
        self.assertTrue(requestName in self.jsonSender.get('user/me')[0])

        self.jsonSender.put('request/%s?status=assignment-approved' % requestName)
        meJSON = self.jsonSender.get('user/me')[0]
        me = json.loads(meJSON)
        self.assertTrue(requestName in me['requests'])
        self.assertEqual(self.jsonSender.put('request/%s?priority=5' % requestName)[1], 200)
        self.assertEqual(self.jsonSender.post('user/me?priority=6')[1], 200)
        self.assertEqual(self.jsonSender.post('group/PeopleLikeMe?priority=7')[1], 200)

        # default priority of group and user of 1
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], 5)
        self.assertEqual(request['ReqMgrRequestorBasePriority'], 6)
        self.assertEqual(request['ReqMgrGroupBasePriority'], 7)
        self.assertEqual(request['RequestPriority'], 5+6+7)

        # only certain transitions allowed
        #self.assertEqual(self.jsonSender.put('request/%s?status=running' % requestName)[1], 400)
        self.assertRaises(HTTPException, self.jsonSender.put,'request/%s?status=running' % requestName)
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['RequestStatus'], 'assignment-approved')

        self.assertTrue(self.jsonSender.put(urllib.quote('assignment/White Sox/%s' % requestName))[1] == 200)
        requestsAndSpecs = self.jsonSender.get(urllib.quote('assignment/White Sox'))[0]
        self.assertTrue(requestName in requestsAndSpecs[0])
        workloadHelper = WMWorkloadHelper()
        workloadHelper.load(requestsAndSpecs[0][1]) 
        self.assertEqual(workloadHelper.getOwner()['Requestor'], "me")
        self.assertTrue(self.jsonSender.get('assignment?request=%s'% requestName)[0] == ['White Sox'])

        agentUrl = 'http://cmssrv96.fnal.gov/workqueue'
        self.jsonSender.put('workQueue/%s?url=%s'% (requestName, urllib.quote(agentUrl)) )
        self.assertEqual(self.jsonSender.get('workQueue/%s' % requestName)[0][0], agentUrl)
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['RequestStatus'], 'acquired')

        self.jsonSender.post('request/%s?events_written=10&files_merged=1' % requestName)
        self.jsonSender.post('request/%s?events_written=20&files_merged=2&percent_success=99.9' % requestName)
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(len(request['RequestUpdates']), 2)
        self.assertEqual(request['RequestUpdates'][0]['files_merged'], 1)
        self.assertEqual(request['RequestUpdates'][1]['events_written'], 20)
        self.assertEqual(request['RequestUpdates'][1]['percent_success'], 99.9)

        message = "The sheriff is near"
        jsonMessage = json.dumps(message)
        self.jsonSender.put('message/%s' % requestName, message)
        messages = self.jsonSender.get('message/%s' % requestName)
        #self.assertEqual(messages[0][0][0], message)
        for status in ['running', 'completed']:
            self.jsonSender.put('request/%s?status=%s' % (requestName, status))

        # campaign
        self.jsonSender.put('campaign/%s' % 'TestCampaign')
        campaigns = self.jsonSender.get('campaign')[0]
        self.assertTrue('TestCampaign' in campaigns.keys())
        self.jsonSender.put('campaign/%s/%s' % ('TestCampaign', requestName))
        requestsInCampaign = self.jsonSender.get('campaign/%s' % 'TestCampaign')[0]
        self.assertTrue(requestName in requestsInCampaign.keys())
        self.jsonSender.delete('request/%s' % requestName)
        return

if __name__=='__main__':
    unittest.main()
