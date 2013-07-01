#!/usr/bin/env python

import os
import unittest
import shutil

#from WMCore_t.ReqMgr_t.Config import Config
from WMCore_t.ReqMgr_t.TestConfig import config
from WMCore.WMSpec.StdSpecs.MonteCarlo import getTestArguments
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend

class ReqMgrTest(RESTBaseUnitTestWithDBBackend):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB sets from environment variable.
    Client DB sets from environment variable.

    This checks whether DS call makes without error and return the results.
    Not the correctness of functions. That will be tested in different module.
    """

    def setUp(self):
        """
        setUP global values
        """
        appport = 19888
        #config = Config(appport, os.getenv("COUCHURL"), False);
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr")])
        self.setSchemaModules([])
        RESTBaseUnitTestWithDBBackend.setUp(self)
        
    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)

    def testRequest(self):
        # add request related REST API test
        #
        #args = getTestArguments()
        #del args["Requestor"]
        #del args["CouchURL"]
        #del args["CouchDBName"]
        
        #TODO : gets the info directly from /test/data/ReqMgr/request
        args ={         
        "CMSSWVersion": "CMSSW_4_1_8",
        "GlobalTag": "START311_V2::All",
        "Campaign": "Campaign-OVERRIDE-ME",
        "RequestString": "RequestString-OVERRIDE-ME",
        "RequestPriority": 1000,
        "FilterEfficiency": 0.0361,
        "ScramArch": "slc5_amd64_gcc434",
        "RequestType": "MonteCarlo",
        "RequestNumEvents": 2000,
        "ConfigCacheID": "4029c9cd130f25d65bdced2311536c52",
        "ConfigCacheUrl": "https://cmsweb-testbed.cern.ch/couchdb",
        "PrimaryDataset": "BdToMuMu_2MuPtFilter_7TeV-pythia6-evtgen",
        "DataPileup": "",
        "MCPileup": "",
        "PrepID": "MCTEST-GEN-0001",
        "Group": "DATAOPS",
        "RunWhitelist": [],
        "TotalTime": 14400, 
        "TimePerEvent": 40,
        "Memory": 2394,
        "SizePerEvent": 512,
        "FirstEvent": 1,
        "FirstLumi": 1
        }

        self.jsonSender.post('data/request', args)
        #TODO: need to make stale option disabled to have the correct record
        print self.jsonSender.get('data/request?status=new&status=assigned')
    
if __name__ == '__main__':

    unittest.main()
