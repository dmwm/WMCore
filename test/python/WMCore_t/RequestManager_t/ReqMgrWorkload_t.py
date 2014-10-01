"""
RequestManager Workload unittest

Tests our ability to create requests of every major type

"""

import os
import sys
import json
import shutil
import urllib
import unittest
from httplib import HTTPException

from nose.plugins.attrib import attr

from WMCore.Services.Requests import JSONRequests
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.Database.CMSCouch import CouchServer

from WMCore_t.RequestManager_t.ReqMgr_t import RequestManagerConfig
from WMCore_t.WMSpec_t.StdSpecs_t.TaskChain_t import makeGeneratorConfig, makeProcessingConfigs
from WMCore_t.RequestManager_t import utils


class ReqMgrWorkloadTest(RESTBaseUnitTest):
    """
    Test that sets up and checks the validations of the various main WMSpec.StdSpecs
    This is mostly a simple set of tests which can be very repetitive.
    
    """

    def setUp(self):
        """
        setUP global values
        Database setUp is done in base class
        
        """
        self.couchDBName = "reqmgr_t_0"
        RESTBaseUnitTest.setUp(self)
        self.testInit.setupCouch("%s" % self.couchDBName, "ConfigCache", "ReqMgr")
        self.testInit.setupCouch("%s_wmstats" % self.couchDBName, "WMStats")
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
        _tearDown_

        Basic tear down of database
        
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
    

    @attr('integration')
    def testA_makeReRecoWorkload(self):
        """
        _makeReRecoWorkload_

        Check that you can make a basic ReReco workload
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema = utils.getAndSetupSchema(self,
                                         userName = userName,
                                         groupName = groupName,
                                         teamName = teamName)
        del schema['GlobalTag']
        configID = self.createConfig(bad = False)
        schema['ConfigCacheID'] = configID
        raises = False
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Error in Workload Validation: Argument GlobalTag is required." in ex.result)
        self.assertTrue(raises)

        schema = utils.getSchema(groupName = groupName,  userName = userName)
        schema['InputDataset'] = '/Nothing'
        raises = False
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            print ex.result
        self.assertTrue(raises)

        schema = utils.getSchema(groupName = groupName,  userName = userName)
        raises = False
        del schema['ConfigCacheID']
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("rror in Workload Validation: Argument ConfigCacheID is required." in ex.result)
        self.assertTrue(raises)

        configID = self.createConfig(bad = True)
        schema = utils.getSchema(groupName = groupName,  userName = userName)
        schema["ConfigCacheID"] = configID
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")

        raises = False
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Error in Workload Validation: "
                            "Duplicate dataTier/filterName combination" in ex.result)
        self.assertTrue(raises)

        schema = utils.getSchema(groupName = groupName,  userName = userName)
        configID = self.createConfig(bad = False)
        schema['ConfigCacheID'] = configID
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")

        try:
            result = self.jsonSender.put('request/testRequest', schema)
        except Exception,ex:
            raise

        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], schema['CMSSWVersion'])
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)

    @attr('integration')
    def testB_Analysis(self):
        """
        _Analysis_

        Test Analysis workflows
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema = utils.getAndSetupSchema(self,
                                         userName = userName,
                                         groupName = groupName,
                                         teamName = teamName)
        schema['RequestType'] = "Analysis"
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
        self.assertTrue(raises)

        # Put the right things in the schema
        # And watch it fail, because we can't open a secure connection to CERN
        # in a unittest well
        schema['RequestorDN'] = 'SomeDN'
        #result = self.jsonSender.put('request/testRequest', schema)
        #requestName = result[0]['RequestName']

        #result = self.jsonSender.get('request/%s' % requestName)
        #request = result[0]
        #self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        #self.assertEqual(request['Group'], groupName)
        #self.assertEqual(request['Requestor'], userName)

    @attr('integration')
    def testD_ReDigi(self):
        """
        _ReDigi_

        Test ReDigi workflows
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        schema['RequestType'] = "ReDigi"
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Error in Workload Validation: Argument StepOneConfigCacheID is required." in ex.result)
        self.assertTrue(raises)

        schema["StepOneConfigCacheID"] = "fakeID"
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Failure to load ConfigCache while validating workload" in ex.result)
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["StepOneConfigCacheID"] = configID
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']
        
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], schema['CMSSWVersion'])
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        self.assertEqual(request['SizePerEvent'], 512)
        self.assertEqual(request['RequestNumEvents'], 0)
        self.assertEqual(request['RequestSizeFiles'], 0)

    @attr('integration')
    def testE_StoreResults(self):
        """
        _StoreResults_

        Test StoreResults workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        schema['RequestType'] = "StoreResults"
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            print ex.result
            self.assertTrue("Error in Workload Validation: Argument CmsPath is required." in ex.result)
        self.assertTrue(raises)

        schema['DbsUrl']            = 'http://fake.dbs.url/dbs'
        schema['CmsPath']           = '/fake/tmp/path'
        schema['AcquisitionEra']    = 'era'
        schema['ProcessingVersion'] = 1
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], schema['CMSSWVersion'])
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        self.assertEqual(request['DbsUrl'], schema['DbsUrl'])
        

    @attr('integration')
    def testF_TaskChain(self):
        """
        _TaskChain_

        Test the monstrous TaskChain workflow
        This will be a long one

        NOTE: This test is so complicated that all I do is
        take code from TaskChain_t and make sure it still
        produces and actual request
        
        """
        couchServer = CouchServer(os.environ["COUCHURL"])
        configDatabase = couchServer.connectDatabase(self.couchDBName)  
        generatorDoc = makeGeneratorConfig(configDatabase)
        processorDocs = makeProcessingConfigs(configDatabase)
        
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema = utils.getSchema(userName = userName)
        schema["CouchURL"] = os.environ["COUCHURL"]
        schema["CouchDBName"] = self.couchDBName
        schema["SiteWhitelist"] = ["T1_CH_CERN", "T1_US_FNAL"]
        schema["TaskChain"] = 5
        chains = {"Task1" : {"TaskName" : "GenSim",
                             "ConfigCacheID" : generatorDoc,
                              "SplittingAlgo"  : "EventBased",
                              "EventsPerJob" : 250,
                              "RequestNumEvents" : 10000,
                              "PrimaryDataset" : "RelValTTBar"},
                  "Task2" : {"TaskName" : "DigiHLT",
                             "InputTask" : "GenSim",
                             "InputFromOutputModule" : "writeGENSIM",
                             "ConfigCacheID" : processorDocs['DigiHLT'],
                             "SplittingAlgo" : "FileBased"},
                  "Task3" : {"TaskName" : "Reco",
                             "InputTask" : "DigiHLT",
                             "InputFromOutputModule" : "writeRAWDIGI",
                             "ConfigCacheID" : processorDocs['Reco'],
                             "SplittingAlgo" : "FileBased"},
                  "Task4" : {"TaskName" : "ALCAReco",
                             "InputTask" : "Reco",
                             "InputFromOutputModule" : "writeALCA",
                             "ConfigCacheID" : processorDocs['ALCAReco'],
                             "SplittingAlgo" : "FileBased"},
                  "Task5" : {"TaskName" : "Skims",
                             "InputTask" : "Reco",
                             "InputFromOutputModule" : "writeRECO",
                             "ConfigCacheID" : processorDocs['Skims'],
                             "SplittingAlgo" : "FileBased",
                             "FilesPerJob" : 10 } }
        schema.update(chains)
        args = utils.getAndSetupSchema(self,
                                       userName = userName,
                                       groupName = groupName,
                                       teamName = teamName)
        schema.update(args)
        
        # this is necessary and after all updates to the schema are made,
        # otherwise this item will get overwritten
        schema['RequestType'] = "TaskChain"
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        

        result = self.jsonSender.put('request/testRequest', schema)

        requestName = result[0]['RequestName']
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], schema['CMSSWVersion'])
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)

        workload = self.loadWorkload(requestName)
        self.assertEqual(workload.data.request.schema.Task1["EventsPerJob"],
                         250)


    @attr('integration')
    def testG_MonteCarloFromGEN(self):
        """
        _MonteCarloFromGEN_

        Test MonteCarloFromGEN workflows
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        schema['RequestType'] = "MonteCarloFromGEN"
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            print ex.result
            self.assertTrue("Error in Workload Validation: Argument ConfigCacheID can't be None" in ex.result)
        self.assertTrue(raises)

        schema["ConfigCacheID"] = "fakeID"
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Failure to load ConfigCache while validating workload" in ex.result)
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["ConfigCacheID"] = configID
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']
        
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], schema['CMSSWVersion'])
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        self.assertEqual(request['SizePerEvent'], 512)
        self.assertEqual(request['RequestNumEvents'], 0)
        self.assertEqual(request['RequestSizeFiles'], 0)


    def testH_MonteCarlo(self):
        """
        _MonteCarlo_

        Test MonteCarlo workflows
        
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        schema['RequestType'] = "MonteCarlo"
        schema['DbsUrl'] = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

        # Set some versions
        schema['ProcessingVersion'] = '2012'
        schema['AcquisitionEra']    = 'ae2012'

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Error in Workload Validation: Argument ConfigCacheID can't be None" in ex.result)
        self.assertTrue(raises)

        schema["ConfigCacheID"] = "fakeID"
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        schema["PrimaryDataset"] = "ReallyFake"
        schema["RequestNumEvents"] = 100
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["ConfigCacheID"] = configID
        schema["FilterEfficiency"] = -0.5

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            # until exception handling is redone (New REST API)
            #self.assertTrue("Negative filter efficiency for MC workflow" in ex.result)
        self.assertTrue(raises)

        schema["FilterEfficiency"] = 1.0
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']
        
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], schema['CMSSWVersion'])
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        self.assertEqual(request['DbsUrl'], schema['DbsUrl'])
        self.assertEqual(request['SizePerEvent'], 512)
        self.assertEqual(request['RequestNumEvents'], 100)
        self.assertEqual(request['RequestSizeFiles'], 0)

    def testJ_Resubmission(self):
        """
        _Resubmission_

        Test Resubmission
        
        """
        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        schema       = utils.getAndSetupSchema(self,
                                               userName = userName,
                                               groupName = groupName,
                                               teamName = teamName)
        schema['RequestType'] = "ReReco"
        configID = self.createConfig()
        schema["ConfigCacheID"] = configID
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")

        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        # user, group schema already set up
        schema = utils.getSchema(groupName = groupName, userName = userName)
        schema['RequestType'] = "Resubmission"
        
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Error in Workload Validation: Argument InitialTaskPath is required." in ex.result)
        self.assertTrue(raises)

        schema["InitialTaskPath"]     = '/%s/DataProcessing' % requestName
        schema["ACDCServer"]          = os.environ.get("COUCHURL")
        schema["ACDCDatabase"]        = self.couchDBName
        schema["CollectionName"]      = "SomeOtherName"

        # Here we just make sure that real result goes through
        result = self.jsonSender.put('request/testRequest', schema)
        resubmitName = result[0]['RequestName']
        result = self.jsonSender.get('request/%s' % resubmitName)

        couchServer = CouchServer(self.testInit.couchUrl)
        reqmgrCouch = couchServer.connectDatabase(self.couchDBName)
        result = reqmgrCouch.loadView('ReqMgr', 'childresubmissionrequests', {}, [requestName])['rows']
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['key'], requestName)
        self.assertEqual(result[0]['id'], resubmitName)

if __name__=='__main__':
    unittest.main()
