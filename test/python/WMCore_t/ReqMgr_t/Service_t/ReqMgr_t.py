#!/usr/bin/env python

import os
import unittest
import shutil
import json

#from WMCore_t.ReqMgr_t.Config import Config
from WMCore_t.ReqMgr_t.TestConfig import config
from WMCore.Wrappers import JsonWrapper
from WMCore.WMBase import getWMBASE
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend

# this needs to move in better location
def insertDataToCouch(couchUrl, couchDBName, data):
    import WMCore.Database.CMSCouch
    server = WMCore.Database.CMSCouch.CouchServer(couchUrl)
    database = server.connectDatabase(couchDBName)
    
    doc = database.commit(data)
    return doc



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
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr"), 
                          (config.views.data.couch_reqmgr_aux_db, None)])
        self.setSchemaModules([])
        RESTBaseUnitTestWithDBBackend.setUp(self)

        # put into ReqMgr auxiliary database under "software" document scram/cmsms
        # which we'll need a little for request injection                
        #Warning: this assumes the same structure in jenkins wmcore_root/test
        requestPath = os.path.join(getWMBASE(), "test", "data", "ReqMgr", "requests")
        mcFile = open(os.path.join(requestPath, "MonteCarlo.json"), 'r')
        self.mcArgs = JsonWrapper.load(mcFile)["createRequest"]
        cmsswDoc = {"_id": "software"}
        cmsswDoc[self.mcArgs["ScramArch"]] =  []
        cmsswDoc[self.mcArgs["ScramArch"]].append(self.mcArgs["CMSSWVersion"])
        insertDataToCouch(os.getenv("COUCHURL"), config.views.data.couch_reqmgr_aux_db, cmsswDoc)        
        
        
    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)


    def testRequest(self):        
        self.jsonSender.post('data/request', self.mcArgs)
        #TODO: need to make stale option disabled to have the correct record
        result = self.jsonSender.get('data/request?status=new&status=assigned&_nostale=true')[0]['result']
        # TODO
        # the above call should return request values. assert some of them
        # against self.mcArgs
        self.jsonSender.get('data/request?prep_id=%s&_nostale=true' % self.mcArgs["PrepID"])[0]['result']
        
        
        
if __name__ == '__main__':
    unittest.main()