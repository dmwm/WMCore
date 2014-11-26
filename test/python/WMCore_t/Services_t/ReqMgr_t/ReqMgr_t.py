#!/usr/bin/env python

import os
import unittest

#from WMCore_t.ReqMgr_t.Config import Config
from WMCore_t.ReqMgr_t.TestConfig import config
from WMCore.Wrappers import JsonWrapper
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WMBase import getWMBASE
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend
from WMCore.ReqMgr.Auth import ADMIN_PERMISSION, DEFAULT_STATUS_PERMISSION, \
                               CREATE_PERMISSION, DEFAULT_PERMISSION, ASSIGN_PERMISSION
from WMCore.REST.Test import fake_authz_headers

# this needs to move in better location
def insertDataToCouch(couchUrl, couchDBName, data):
    import WMCore.Database.CMSCouch
    server = WMCore.Database.CMSCouch.CouchServer(couchUrl)
    database = server.connectDatabase(couchDBName)
    
    doc = database.commit(data)
    return doc

def getAuthHeader(hmacData, reqAuth):
    roles = {}
    for role in reqAuth['role']:
        roles[role] = {'group': reqAuth['group']}
        
    return fake_authz_headers(hmacData, roles = roles, format = "dict") 


class ReqMgrTest(RESTBaseUnitTestWithDBBackend):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB sets from environment variable.
    Client DB sets from environment variable.

    This checks whether DS call makes without error and return the results.
    Not the correctness of functions. That will be tested in different module.
    
    """
    
    def resultLength(self, response, format="dict"):
        # result is dict format
        if format == "dict":
            return len(response[0]['result'][0])
        elif format == "list":
            return  len(response[0]['result'])
        
        
    def setFakeDN(self):
        # put into ReqMgr auxiliary database under "software" document scram/cmsms
        # which we'll need a little for request injection                
        #Warning: this assumes the same structure in jenkins wmcore_root/test
        self.admin_header = getAuthHeader(self.test_authz_key.data, ADMIN_PERMISSION)
        self.create_header = getAuthHeader(self.test_authz_key.data, CREATE_PERMISSION)
        self.default_header = getAuthHeader(self.test_authz_key.data, DEFAULT_PERMISSION)
        self.assign_header = getAuthHeader(self.test_authz_key.data, ASSIGN_PERMISSION)
        self.default_status_header = getAuthHeader(self.test_authz_key.data, DEFAULT_STATUS_PERMISSION)
        
    def setUp(self):
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr"), 
                          (config.views.data.couch_reqmgr_aux_db, None)])
        self.setSchemaModules([])
        
        RESTBaseUnitTestWithDBBackend.setUp(self)

        self.setFakeDN()
        
        requestPath = os.path.join(getWMBASE(), "test", "data", "ReqMgr", "requests")
        rerecoFile = open(os.path.join(requestPath, "ReReco.json"), 'r')
        
        rerecoArgs = JsonWrapper.load(rerecoFile)
        self.rerecoCreateArgs = rerecoArgs["createRequest"]
        self.rerecoAssignArgs = rerecoArgs["assignRequest"]
        cmsswDoc = {"_id": "software"}
        cmsswDoc[self.rerecoCreateArgs["ScramArch"]] =  []
        cmsswDoc[self.rerecoCreateArgs["ScramArch"]].append(self.rerecoCreateArgs["CMSSWVersion"])
        insertDataToCouch(os.getenv("COUCHURL"), config.views.data.couch_reqmgr_aux_db, cmsswDoc) 
        self.reqSvc = ReqMgr(self.jsonSender["host"]) 
        self.reqSvc._noStale = True      
        self.reqSvc['requests'].additionalHeaders = self.create_header
        
    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)


    def testRequestSimpleCycle(self):
        """
        test request cycle with one request without composite get condition.
        post, get, put
        """
        
        # test post method
        response = self.reqSvc.insertRequests(self.rerecoCreateArgs)
        from pprint import pprint
        pprint(response)
        self.assertEqual(len(response), 1)
        requestName = response[0]['RequestName']
        
        ## test get method
        # get by name
        response = self.reqSvc.getRequestByNames(requestName)
        self.assertEqual(len(response), 1)
        
        # get by status
        response = self.reqSvc.getRequestByStatus('new')
        self.assertEqual(len(response), 1)
        

        self.reqSvc.updateRequestStatus(requestName, 'assignment-approved')
        response = self.reqSvc.getRequestByStatus('assignment-approved')
        self.assertEqual(len(response), 1)
    
if __name__ == '__main__':
    unittest.main()
