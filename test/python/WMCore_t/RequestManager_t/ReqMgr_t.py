#!/usr/bin/env python

"""
RequestManager unittest

Tests the functions of the REST API

"""


import os
import json
import shutil
import urllib
import tempfile
import unittest
import threading
from httplib import HTTPException

from nose.plugins.attrib import attr

from WMCore.Services.Requests   import JSONRequests
from WMCore.Wrappers            import JsonWrapper as json
from WMCore.WMSpec.WMWorkload   import WMWorkloadHelper

from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException

# RequestDB Interfaces
from WMCore.RequestManager.RequestDB.Interface.Request import GetRequest
from WMCore.RequestManager.RequestDB.Interface.Admin   import SoftwareManagement

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup  import DefaultConfig

from WMCore_t.RequestManager_t import utils



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
        self._setupAssign()

    def setupCouchDatabase(self, dbName):
        self.UnitTests.views.active.rest.configDBName   = dbName
        self.UnitTests.views.active.rest.workloadDBName = dbName
        self.UnitTests.views.active.rest.wmstatDBName   = "%s_wmstats" % dbName

    def _setupAssign(self):
        self.UnitTests.views.active.rest.sitedb  = "https://cmsweb.cern.ch/sitedb/json/index/"
        
        
        
class ReqMgrTest(RESTBaseUnitTest):
    """
    _ReqMgrTest_

    Basic test for the ReqMgr services.
    Setup is done off-screen in RESTBaseUnitTest - this makes
    things confusing
    
    """

    def setUp(self):
        """
        setUP global values
        Database setUp is done in base class
        """
        self.couchDBName = "reqmgr_t_0"
        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch("%s" % self.couchDBName,
                                 "GroupUser", "ConfigCache")
        self.testInit.setupCouch("%s_wmstats" % self.couchDBName,
                                 "WMStats")
        reqMgrHost = self.config.getServerUrl()
        self.jsonSender = JSONRequests(reqMgrHost)


    def initialize(self):
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()
        self.config.setupCouchDatabase(dbName = self.couchDBName)
        self.config.setPort(12888)
        self.schemaModules = ["WMCore.RequestManager.RequestDB"]


    def tearDown(self):
        """
        tearDown 

        Tear down everything
        
        """
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
        users = self.jsonSender.get('group/PeopleLikeMe')[0]['users']
        self.assertTrue('me' in users)
        groups = self.jsonSender.get('user/me')[0]['groups']
        self.assertTrue('PeopleLikeMe' in groups)
        groups2 = self.jsonSender.get('group?user=me')[0]
        self.assertTrue('PeopleLikeMe' in groups2)

        if 'White Sox' in self.jsonSender.get('team')[0]:
            self.jsonSender.delete(urllib.quote('team/White Sox'))
        self.assertFalse('White Sox' in self.jsonSender.get('team')[0])
        self.assertEqual(self.jsonSender.put(urllib.quote('team/White Sox'))[1], 200)
        self.assertTrue('White Sox' in self.jsonSender.get('team')[0])

        # some foreign key stuff to dealwith
        schema = utils.getSchema()
        version = "version/" + schema["CMSSWVersion"]
        self.assertTrue(self.jsonSender.put(version)[1] == 200)
        self.assertTrue(schema["CMSSWVersion"] in self.jsonSender.get('version')[0])
        

    @attr("integration")
    def testB_ReReco(self):
        """
        _ReReco_

        Try a basic ReReco workflow
        
        """
        schema = utils.getAndSetupSchema(self)
        schema['RequestNumEvents'] = 100
        schema['RequestEventSize'] = 101
        self.doRequest(schema)


    def doRequest(self, schema):
        """
        _doRequest_

        Run all tests on a basic ReReco workflow
        
        """
        requestName = schema['RequestName']
        self.assertRaises(HTTPException, self.jsonSender.delete, 'request/%s' % requestName)
        result = self.jsonSender.put('request/%s' % (requestName), schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        self.assertEqual(self.jsonSender.get('request/%s' % requestName)[0]['RequestName'], requestName)

        self.jsonSender.put('request/%s?status=assignment-approved' % requestName)
        me = self.jsonSender.get('user/me')[0]
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

        # Check LFN Bases
        self.assertEqual(request['UnmergedLFNBase'], '/store/unmerged')
        self.assertEqual(request['MergedLFNBase'], '/store/data')

        # Check random other
        self.assertEqual(request['CustodialSite'], 'US_T1_FNAL')

        # Check Num events
        self.assertEqual(request['RequestNumEvents'], 100)
        self.assertEqual(request['RequestEventSize'], 101)

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
        self.assertEqual(self.jsonSender.get('assignment?request=%s'% requestName)[0], ['White Sox'])
        self.assertEqual(self.jsonSender.get('request/%s' % requestName)[0]['teams'], ['White Sox'])

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

        req = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(req['Campaign'], 'TestCampaign')
        self.jsonSender.delete('request/%s' % requestName)
        

    @attr("integration")
    def testC_404Errors(self):
        """
        _404Errors_

        Do some things that generate 404 errors.  This
        should be limited to requests for objects that do not
        exist.
        """
        badName = 'ThereIsNoWayThisNameShouldExist'

        # First, try to find a non-existant request
        # This should throw a 404 error.
        # The request name should not be in it
        self.checkForError(cls = 'request', badName = badName, exitCode = 404,
                           message = 'Given requestName not found')

        # Now look for non-existant user
        self.checkForError(cls = 'user', badName = badName, exitCode = 404,
                           message = 'Cannot find user')

        # Now try non-existant group
        self.checkForError(cls = 'group', badName = badName, exitCode = 404,
                           message = "Cannot find group/group priority")

        # Now try non-existant campaign
        self.checkForError(cls = 'campaign', badName = badName, exitCode = 404,
                           message = "Cannot find campaign")

        # Now try invalid message
        # This raises a requestName error becuase it searches for the request
        self.checkForError(cls = 'message', badName = badName, exitCode = 404,
                           message = "Given requestName not found", testEmpty = False)

        # Check for assignments (no teams or requests)
        # This raises a team error because it tries to load teams out first
        self.checkForError(cls = 'assignment', badName = badName, exitCode = 404,
                           message = 'Cannot find team')


    @attr("integration")
    def testD_400Errors(self):
        """
        _400Errors_

        These are failures created by invalid input, such as sending
        args to a request when it doesn't accept any.  They should
        generatore 400 Errors
        
        """
        badName = 'ThereIsNoWayThisNameShouldExist'
        
        # Attempt to send arguments to a function that doesn't accept them.
        self.checkForError(cls = 'team', badName = badName, exitCode = 400,
                           message = "Invalid input: Arguments added where none allowed")

        # Recheck for versions
        self.checkForError(cls = 'version', badName = badName, exitCode = 400,
                           message = "Invalid input: Arguments added where none allowed")

        # Break the validation
        self.checkForError(cls = 'user', badName = '!', exitCode = 400,
                           message = 'Invalid input: Input data failed validation')
        

    def checkForError(self, cls, badName, exitCode, message, testEmpty = True):
        """
        _checkForError_

        Generic function for checking for errors in JSON commands

        Does a basic check on type cls searching for name badName which hopefull
        does not exist.

        Checks to make sure that it exits with code exitCode, and that
        the error contains the string message.

        Also checks to make sure that name badName is NOT in the output

        testEmpty for those that don't handle calls to the main (i.e., who require
        an argument)
        
        """
        raises = False
        # First assert that the test to be tested is empty
        if testEmpty:
            result = self.jsonSender.get(cls)
            self.assertTrue(type(result[0]) in [type([]), type({})])
        # Next, test
        try:
            result = self.jsonSender.get('%s/%s' % (cls, badName))
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, exitCode)
            self.assertTrue(message in ex.result)
            self.assertFalse(badName in ex.result)
        self.assertTrue(raises)


    @attr("integration")
    def testE_CheckStatusChanges(self):
        """
        _CheckStatusChanges_
        
        Check status changes for a single request.  See whether
        we can move the request through the proper chain.  Figure
        out what happens when we fail.
        
        """
        myThread = threading.currentThread()

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        # There should only be one request in the DB
        result = GetRequest.requestID(requestName = requestName)
        self.assertEqual(result, 1)
        result = self.jsonSender.get('request/%s' % requestName)
        self.assertEqual(result[0]['Group'], groupName)
        self.assertEqual(result[0]['Requestor'], userName)

        # Let's see what we can do in terms of setting status
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'new')

        # Let's try an illegal status change, just for the hell of it
        raises = False
        try:
            self.jsonSender.put('request/%s?status=negotiating' % requestName)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 403)
            self.assertTrue('Failed to change status' in ex.result)
            self.assertFalse(requestName in ex.result)
        self.assertTrue(raises)

        # Now, let's try a totally bogus status
        raises = False
        try:
            self.jsonSender.put('request/%s?status=bogus' % requestName)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 403)
            self.assertTrue('Failed to change status' in ex.result)
            self.assertFalse(requestName in ex.result)
        self.assertTrue(raises)

        # We should still be in new
        result = self.jsonSender.get('request/%s' % requestName)
        self.assertEqual(result[0]['RequestStatus'], 'new')

        # Let's go on in a full loop
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'testing-approved')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'testing')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'tested')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'assignment-approved')

        # This should fail, as you cannot assign a request without a team
        raises = False
        try:
            self.changeStatusAndCheck(requestName = requestName,
                                      statusName  = 'assigned')
        except HTTPException, ex:
            raises = True
            self.assertTrue('Cannot change status without a team' in ex.result)
        self.assertTrue(raises)

        self.jsonSender.put(urllib.quote('assignment/%s/%s' % (teamName, requestName)))
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'negotiating')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'acquired')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'running')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'completed')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'closed-out')


    def changeStatusAndCheck(self, requestName, statusName):
        """
        _changeStatusAndCheck_

        Change the status of a request and make sure that
        the request actually did it.
        
        """
        self.jsonSender.put('request/%s?status=%s' % (requestName, statusName))
        result = self.jsonSender.get('request/%s' % requestName)
        self.assertEqual(result[0]['RequestStatus'], statusName)
        

    def loadWorkload(self, requestName):
        """
        _loadWorkload_

        Load the workload from couch after we've saved it there.
        
        """
        workload = WMWorkloadHelper()
        url      = '%s/%s/%s/spec' % (os.environ['COUCHURL'], self.couchDBName,
                                      requestName)
        workload.load(url)
        return workload


    def testF_TestWhitelistBlacklist(self):
        """
        _TestWhitelistBlacklist_

        Test whether or not we can assign the block/run blacklist/whitelist
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        schema['RunWhitelist'] = [1, 2, 3]
        schema['RunBlacklist'] = [4, 5, 6]
        schema['BlockWhitelist'] = ['/dataset/dataset/dataset#alpha']
        schema['BlockBlacklist'] = ['/dataset/dataset/dataset#beta']

        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.data.tasks.DataProcessing.input.dataset.runs.whitelist, schema['RunWhitelist'])
        self.assertEqual(workload.data.tasks.DataProcessing.input.dataset.runs.blacklist, schema['RunBlacklist'])
        self.assertEqual(workload.data.tasks.DataProcessing.input.dataset.blocks.whitelist, schema['BlockWhitelist'])
        self.assertEqual(workload.data.tasks.DataProcessing.input.dataset.blocks.blacklist, schema['BlockBlacklist'])

        req = self.jsonSender.get('request/%s' % requestName)
        self.assertTrue(req[0].has_key('Site Blacklist'))
        self.assertTrue(req[0].has_key('Site Whitelist'))

        schema['BlockBlacklist'] = {'1': '/dataset/dataset/dataset#beta'}

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Bad Run list of type " in ex.result)
            pass
        self.assertTrue(raises)

        schema['BlockBlacklist'] = ['/dataset/dataset/dataset#beta']
        schema['RunWhitelist']   = {'1': '/dataset/dataset/dataset#beta'}

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Bad Run list of type " in ex.result)
            pass
        self.assertTrue(raises)

        schema['RunWhitelist'] = ['hello', 'how', 'are', 'you']
        try:
            raises = True
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Given runList without integer run numbers" in ex.result)
            pass
        self.assertTrue(raises)


    def testG_AddDuplicateUser(self):
        """
        _AddDuplicateUser_

        Test and see if we get a sensible error when adding a duplicate user.
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        raises = False
        try:
            self.jsonSender.put('group/%s/%s' % (groupName, userName))
        except HTTPException, ex:
            self.assertTrue("User/Group Already Linked in DB" in ex.result)
            self.assertEqual(ex.status, 400)
            raises = True
        self.assertTrue(raises)


    def testH_RemoveSoftwareVersion(self):
        """
        _RemoveSoftwareVersion_

        Remove the software version after submitting the request.  See what that does.
        
        """
        myThread = threading.currentThread()
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        req = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(req['SoftwareVersions'], [schema["CMSSWVersion"]])

        # Delete software versions and make sure they're gone from the DB
        SoftwareManagement.removeSoftware(softwareName = schema["CMSSWVersion"],
                                          scramArch = schema["ScramArch"])
        versions = myThread.dbi.processData("SELECT * FROM reqmgr_software")[0].fetchall()
        self.assertEqual(versions, [])
        assocs = myThread.dbi.processData("SELECT * FROM reqmgr_software_dependency")[0].fetchall()
        self.assertEqual(assocs, [])
        
        req = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(req['SoftwareVersions'], [schema["CMSSWVersion"]])
        

    def testI_CheckConfigIDs(self):
        """
        _CheckConfigIDs_

        Check to see if we can pull out the ConfigIDs by request
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        # Set some versions
        schema['ProcessingVersion'] = '2012'
        schema['AcquisitionEra']    = 'ae2012'
        schema["PrimaryDataset"]    = "ReallyFake"
        schema["RequestNumEvents"]  = 100

        configID = self.createConfig()
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        schema["ConfigCacheID"] = configID
        schema["InputDatasets"]     = ['/MinimumBias/Run2010B-RelValRawSkim-v1/RAW']

        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('configIDs?prim=MinimumBias&proc=Commissioning10-v4&tier=RAW')[0]
        self.assertTrue(requestName in result.keys())
        self.assertTrue(configID in result[requestName][0])
        
        
    def testJ_CheckRequestCloning(self):
        myThread = threading.currentThread()
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        result = self.jsonSender.put("request", schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]["RequestName"]
        # AcquisitionEra is returned here, but in fact on server is not stored until assign
        # see below - when retrieving the request it's None ...
        # [just to mark ReqMgr1 peculiarities]
        self.assertTrue(schema["AcquisitionEra"], result[0]["AcquisitionEra"])        
        # set some non-default priority
        # when cloning a request which had some non default priority,
        # the priority values were lost when creating a cloned request, the
        # default values were lost. Change it here to specifically catch this case.
        priority = 300
        result = self.jsonSender.put("request/%s?priority=%s" % (requestName, priority))        
        self.assertEqual(result[1], 200)
        # get the original request from the server, although the variable result
        # shall have the same stuff in
        response = self.jsonSender.get("request/%s" % requestName)
        origRequest = response[0]
        self.assertEquals(origRequest["AcquisitionEra"], "None") # was not stored, see above
        # test that the priority was correctly set in the brand-new request
        self.assertEquals(origRequest["ReqMgrRequestBasePriority"], priority)
        
        # test cloning not existing request
        self.assertRaises(HTTPException, self.jsonSender.put, "clone/%s" % "NotExistingRequestName")
        # correct attempt to clone the request
        # this is the new request, it'll have different name
        result = self.jsonSender.put("clone/%s" % requestName)
        # get the cloned request from the server
        respose = self.jsonSender.get("request/%s" % result[0]["RequestName"])
        clonedRequest = respose[0]
        # these request arguments shall differ in the cloned request:
        toDiffer = ["RequestName", "ReqMgrRequestID", "RequestWorkflow", "RequestStatus"]
        for differ in toDiffer:
            self.assertNotEqual(origRequest[differ], clonedRequest[differ])
        # check the desired status of the cloned request
        self.assertEquals(clonedRequest["RequestStatus"], "assignment-approved",
                          "Cloned request status should be 'assignment-approved', not '%s'." %
                          clonedRequest["RequestStatus"])
        # don't care about these two (they will likely be the same in the unittest
        # since the brand new request injection as well as the cloning probably
        # happen at roughly the same time)
        toDiffer.extend(["RequestDate", "timeStamp"])
        for differ in toDiffer:
            del origRequest[differ]
            del clonedRequest[differ]
        # check the request dictionaries
        self.assertEquals(len(origRequest), len(clonedRequest))
        for k1, k2 in zip(sorted(origRequest.keys()), sorted(clonedRequest.keys())):
            msg = ("Request values: original: %s: %s cloned: %s: %s differ" %
                   (k1, origRequest[k1], k2, clonedRequest[k2]))
            self.assertEqual(origRequest[k1], clonedRequest[k2], msg)
        
        

if __name__=='__main__':
    unittest.main()