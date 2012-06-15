#!/usr/bin/env python

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

from httplib                  import HTTPException
from WMCore.Services.Requests import JSONRequests

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMCore.WMSpec.StdSpecs.ReReco       import getTestArguments
from WMCore.WMSpec.WMWorkload            import WMWorkloadHelper
from WMCore.Cache.WMConfigCache          import ConfigCache, ConfigCacheException
from WMCore.Database.CMSCouch            import CouchServer

import WMCore.WMSpec.StdSpecs.ReReco as ReReco
from nose.plugins.attrib import attr

from WMCore_t.RequestManager_t.ReqMgr_t import RequestManagerConfig, getRequestSchema


class ReqMgrWorkloadTest(RESTBaseUnitTest):
    """
    _ReqMgrWorkloadTest_

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
        self.testInit.setupCouch("%s" % self.couchDBName,
                                 "GroupUser", "ConfigCache")

        reqMgrHost      = self.config.getServerUrl()
        self.jsonSender = JSONRequests(reqMgrHost)

        return

    def initialize(self):
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()
        self.config.setupCouchDatabase(dbName = self.couchDBName)
        self.config.setPort(12888)
        self.schemaModules = ["WMCore.RequestManager.RequestDB"]
        return

    def tearDown(self):
        """
        _tearDown_

        Basic tear down of database
        """

        RESTBaseUnitTest.tearDown(self)
        self.testInit.tearDownCouch()
        return

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

    def setupSchema(self, groupName = 'PeopleLikeMe',
                    userName = 'me', teamName = 'White Sox',
                    CMSSWVersion = 'CMSSW_3_5_8',
                    typename = 'ReReco', setupDB = True,
                    scramArch = 'slc5_ia32_gcc434'):
        """
        _setupSchema_

        Set up a test schema so that we can run a test request.
        Standardization!
        """
        if setupDB:
            self.jsonSender.put('user/%s?email=me@my.com' % userName)
            self.jsonSender.put('group/%s' % groupName)
            try:
                self.jsonSender.put('group/%s/%s' % (groupName, userName))
            except:
                # Done this already
                pass
            self.jsonSender.put(urllib.quote('team/%s' % teamName))
            self.jsonSender.put('version/%s/%s' % (CMSSWVersion, scramArch))

        schema = ReReco.getTestArguments()
        schema['RequestName'] = 'TestReReco'
        schema['RequestType'] = typename
        schema['CmsPath'] = "/uscmst1/prod/sw/cms"
        schema['Requestor'] = '%s' % userName
        schema['Group'] = '%s' % groupName

        return schema

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
        CMSSWVersion = 'CMSSW_3_5_8'
        

        # Okay, we can make one.  Shouldn't surprise us.  Let's try
        # and make a bad one.
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        setupDB = True)
        del schema['GlobalTag']
        raises = False
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field GlobalTag in workload validation" in ex.result)
        self.assertTrue(raises)

        
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        setupDB = False)
        schema['InputDataset'] = '/Nothing'
        raises = False
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Bad value for InputDataset" in ex.result)
        self.assertTrue(raises)

        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        setupDB = False)
        raises = False
        del schema['ProcScenario']
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("No Scenario or Config in Processing Request!" in ex.result)
        self.assertTrue(raises)

        
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        setupDB = False)

        configID = self.createConfig(bad = True)
        schema["ProcConfigCacheID"] = configID
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")

        raises = False
        try:
            self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Error in Workload Validation: Duplicate dataTier/filterName combination" in ex.result)
        self.assertTrue(raises)

        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        setupDB = False)
        try:
            result = self.jsonSender.put('request/testRequest', schema)
        except Exception,ex:
            raise

        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        
        return

    @attr('integration')
    def testB_Analysis(self):
        """
        _Analysis_

        Test Analysis workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "Analysis")

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            pass
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
        return


    @attr('integration')
    def testC_DataProcessing(self):
        """
        _DataProcessing_

        Test DataProcessing workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "DataProcessing")

        del schema['ProcScenario']
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("No Scenario or Config in Processing Request!" in ex.result)
            pass
        self.assertTrue(raises)

        # Now we have to make ourselves a configCache
        configID = self.createConfig()
        schema["ProcConfigCacheID"] = configID
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        return

    @attr('integration')
    def testD_ReDigi(self):
        """
        _ReDigi_

        Test ReDigi workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "ReDigi")

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field StepOneConfigCacheID in workload validation" in ex.result)
            pass
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
            pass
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["StepOneConfigCacheID"] = configID
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']
        
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)

        return

    @attr('integration')
    def testE_StoreResults(self):
        """
        _StoreResults_

        Test StoreResults workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "StoreResults")

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field DbsUrl in workload validation" in ex.result)
            pass
        self.assertTrue(raises)

        schema['DbsUrl']            = 'http://fake.dbs.url/dbs'
        schema['AcquisitionEra']    = 'era'
        schema['ProcessingVersion'] = 1
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        return

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

        from WMCore_t.WMSpec_t.StdSpecs_t.TaskChain_t import makeGeneratorConfig, makeProcessingConfigs
        couchServer = CouchServer(os.environ["COUCHURL"])
        configDatabase = couchServer.connectDatabase(self.couchDBName)  
        generatorDoc = makeGeneratorConfig(configDatabase)
        processorDocs = makeProcessingConfigs(configDatabase)

        schema = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": os.environ["COUCHURL"],
            "CouchDBName": self.couchDBName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "TaskChain" : 5,
            "Task1" :{
                "TaskName" : "GenSim",
                "ConfigCacheID" : generatorDoc, 
                "SplittingAlgorithm"  : "EventBased",
                "SplittingArguments" : {"events_per_job" : 250},
                "RequestNumEvents" : 10000,
                "Seeding" : "Automatic",
                "PrimaryDataset" : "RelValTTBar",
            },
            "Task2" : {
                "TaskName" : "DigiHLT",
                "InputTask" : "GenSim",
                "InputFromOutputModule" : "writeGENSIM",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            },
            "Task3" : {
                "TaskName" : "Reco",
                "InputTask" : "DigiHLT",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['Reco'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            },
            "Task4" : {
                "TaskName" : "ALCAReco",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeALCA",
                "ConfigCacheID" : processorDocs['ALCAReco'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            
            },
            "Task5" : {
                "TaskName" : "Skims",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeRECO",
                "ConfigCacheID" : processorDocs['Skims'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 10 },            
            }
            
        }

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        args         = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "TaskChain")

        schema.update(args)
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")

        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)

        workload = self.loadWorkload(requestName)
        self.assertEqual(workload.data.request.schema.Task1.SplittingArguments,
                         {'events_per_job': 250})

        return


    @attr('integration')
    def testG_MonteCarloFromGEN(self):
        """
        _MonteCarloFromGEN_

        Test MonteCarloFromGEN workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "MonteCarloFromGEN")

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field ProcConfigCacheID in workload validation" in ex.result)
            pass
        self.assertTrue(raises)

        schema["ProcConfigCacheID"] = "fakeID"
        schema["CouchDBName"] = self.couchDBName
        schema["CouchURL"]    = os.environ.get("COUCHURL")
        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Failure to load ConfigCache while validating workload" in ex.result)
            pass
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["ProcConfigCacheID"] = configID
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']
        
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)

        return

    def testH_MonteCarlo(self):
        """
        _MonteCarlo_

        Test MonteCarlo workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "MonteCarlo")

        # Set some versions
        schema['ProcessingVersion'] = '2012'
        schema['AcquisitionEra']    = 'ae2012'
        schema['FirstEvent'] = 1
        schema['FirstLumi'] = 1

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field ProcConfigCacheID in workload validation" in ex.result)
            pass
        self.assertTrue(raises)

        schema["ProcConfigCacheID"] = "fakeID"
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
            self.assertTrue("Failure to load ConfigCache while validating workload" in ex.result)
            pass
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["ProcConfigCacheID"] = configID
        schema["FilterEfficiency"] = -0.5

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Negative filter efficiency for MC workflow" in ex.result)
            pass
        self.assertTrue(raises)

        schema["FilterEfficiency"] = 1.0
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']
        
        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        self.assertEqual(request['ProcessingVersion'], schema['ProcessingVersion'])
        self.assertEqual(request['AcquisitionEra'], schema['AcquisitionEra'])

        return


    def testI_RelValMC(self):
        """
        _RelValMC_

        Test RelValMC workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "RelValMC")

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field PrimaryDataset in workload validation" in ex.result)
            pass
        self.assertTrue(raises)

        schema["GenConfigCacheID"]        = "fakeID"
        schema["StepOneConfigCacheID"]    = "fakeID"
        schema["StepTwoConfigCacheID"]    = "fakeID"
        schema["CouchDBName"]             = self.couchDBName
        schema["CouchURL"]                = os.environ.get("COUCHURL")
        schema["PrimaryDataset"]          = "ReallyFake"
        schema["RequestNumEvents"]        = 100
        schema["GenOutputModuleName"]     = "ThisIsAName"
        schema["StepOneOutputModuleName"] = "ThisIsAName"

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Failure to load ConfigCache while validating workload" in ex.result)
            pass

        configID = self.createConfig()
        schema["GenConfigCacheID"]     = configID
        schema["StepOneConfigCacheID"] = configID
        schema["StepTwoConfigCacheID"] = configID
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)


        return

    def testJ_Resubmission(self):
        """
        _Resubmission_

        Test Resubmission
        Use an utterly bogus CMSSWVersion since it shouldn't check that anyway
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "DataProcessing")
        schema['ProdScenario'] = 'pp'
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = 'CMSSW_1_0_0',
                                        typename = "Resubmission")
        

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field OriginalRequestName" in ex.result)
            pass
        self.assertTrue(raises)

        schema["OriginalRequestName"] = requestName
        schema["InitialTaskPath"]     = '/%s/DataProcessing' % requestName
        schema["ACDCServer"]          = os.environ.get("COUCHURL")
        schema["ACDCDatabase"]        = self.couchDBName

        # Here we just make sure that real result goes through
        result = self.jsonSender.put('request/testRequest', schema)
        resubmitName = result[0]['RequestName']
        result = self.jsonSender.get('request/%s' % resubmitName)
        request = result[0]
        
        return

    def testK_LHEStepZero(self):
        """
        _LHEStepZero_

        Test LHEStepZero workflows
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion,
                                        typename = "LHEStepZero")

        # Set some versions
        schema['ProcessingVersion'] = '2012'
        schema['AcquisitionEra']    = 'ae2012'
        schema['FirstEvent'] = 1
        schema['FirstLumi'] = 1

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Missing required field ProcConfigCacheID in workload validation" in ex.result)
            pass
        self.assertTrue(raises)

        schema["ProcConfigCacheID"] = "fakeID"
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
            self.assertTrue("Failure to load ConfigCache while validating workload" in ex.result)
            pass
        self.assertTrue(raises)

        configID = self.createConfig()
        schema["ProcConfigCacheID"] = configID
        schema["FilterEfficiency"] = -0.5

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Negative filter efficiency for MC workflow" in ex.result)
            pass
        self.assertTrue(raises)

        schema["FilterEfficiency"] = 1.0

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("No events per lumi information was entered" in ex.result)
            pass
        self.assertTrue(raises)

        schema["EventsPerLumi"] = "-10"

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("The events per lumi input from the user is invalid, only positive numbers allowed" in ex.result)
            pass
        self.assertTrue(raises)

        schema["EventsPerLumi"] = "101"

        try:
            raises = False
            result = self.jsonSender.put('request/testRequest', schema)
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("More events per lumi than total events requested" in ex.result)
            pass
        self.assertTrue(raises)

        schema["EventsPerLumi"] = "20"
        result = self.jsonSender.put('request/testRequest', schema)
        requestName = result[0]['RequestName']

        result = self.jsonSender.get('request/%s' % requestName)
        request = result[0]
        self.assertEqual(request['CMSSWVersion'], CMSSWVersion)
        self.assertEqual(request['Group'], groupName)
        self.assertEqual(request['Requestor'], userName)
        self.assertEqual(request['ProcessingVersion'], schema['ProcessingVersion'])
        self.assertEqual(request['AcquisitionEra'], schema['AcquisitionEra'])

        return


if __name__=='__main__':
    unittest.main()
