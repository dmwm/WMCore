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
        
        self.default_status_header = getAuthHeader(self.test_authz_key.data, DEFAULT_STATUS_PERMISSION)
        
        requestPath = os.path.join(getWMBASE(), "test", "data", "ReqMgr", "requests")
        rerecoFile = open(os.path.join(requestPath, "ReReco.json"), 'r')
        rerecoArgs = JsonWrapper.load(rerecoFile)
        self.rerecoCreateArgs = rerecoArgs["createRequest"]
        self.rerecoAssignArgs = rerecoArgs["assignRequest"]
        cmsswDoc = {"_id": "software"}
        cmsswDoc[self.rerecoCreateArgs["ScramArch"]] =  []
        cmsswDoc[self.rerecoCreateArgs["ScramArch"]].append(self.rerecoCreateArgs["CMSSWVersion"])
        insertDataToCouch(os.getenv("COUCHURL"), config.views.data.couch_reqmgr_aux_db, cmsswDoc)        
        
        
    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)


    def getRequestWithNoStale(self, query):
        prefixWithNoStale = "data/request?_nostale=true&"
        return self.jsonSender.get(prefixWithNoStale + query, 
                                   incoming_headers=self.default_header)
    
    def postRequestWithAuth(self, data):
        return self.jsonSender.post('data/request', data, incoming_headers=self.create_header)
    
    def putRequestWithAuth(self, requestName, data):
        """
        WMCore.REST doesn take query for the put request.
        data need to send on the body
        """
        return self.jsonSender.put('data/request/%s' % requestName, data, 
                                     incoming_headers=self.assign_header)
        
    def cloneRequestWithAuth(self, requestName):
        """
        WMCore.REST doesn take query for the put request.
        data need to send on the body
        """
        return self.jsonSender.put('data/request/clone/%s' % requestName, {},
                                     incoming_headers=self.assign_header)
    
    def resultLength(self, response, format="dict"):
        # result is dict format
        if format == "dict":
            return len(response[0]['result'][0])
        elif format == "list":
            return  len(response[0]['result'])
    
    def insertRequest(self, args):
        # test post method
        respond = self.postRequestWithAuth(self.rerecoCreateArgs)
        self.assertEqual(respond[1], 200)
        
        requestName = respond[0]['result'][0]['RequestName']
        return requestName
        
    def testRequestSimpleCycle(self):
        """
        test request cycle with one request without composite get condition.
        post, get, put
        """
        
        # test post method
        requestName = self.insertRequest(self.rerecoCreateArgs)
        
        ## test get method
        # get by name
        respond = self.getRequestWithNoStale('name=%s' % requestName)
        self.assertEqual(respond[1], 200, "get by name")
        self.assertEqual(self.resultLength(respond), 1)
        
        # get by status
        respond = self.getRequestWithNoStale('status=new')
        self.assertEqual(respond[1], 200, "get by status")
        self.assertEqual(self.resultLength(respond), 1)
        
        #this create cache
        # need to find the way to reste Etag or not getting from the cache 
#         respond = self.getRequestWithNoStale('status=assigned')
#         self.assertEqual(respond[1], 200, "get by status")
#         self.assertEqual(self.resultLength(respond), 0)
        
        # get by prepID
        respond = self.getRequestWithNoStale('prep_id=%s' % self.rerecoCreateArgs["PrepID"])
        self.assertEqual(respond[1], 200)
        self.assertEqual(self.resultLength(respond), 1)
        #import pdb
        #pdb.set_trace()
        respond = self.getRequestWithNoStale('campaign=%s' % self.rerecoCreateArgs["Campaign"])
        self.assertEqual(respond[1], 200)
        self.assertEqual(self.resultLength(respond), 1)
        
        respond = self.getRequestWithNoStale('inputdataset=%s' % self.rerecoCreateArgs["InputDataset"])
        self.assertEqual(respond[1], 200)
        self.assertEqual(self.resultLength(respond), 1)
        
        
        # test put request with just status change
        data = {'RequestStatus': 'assignment-approved'}
        self.putRequestWithAuth(requestName, data)
        respond = self.getRequestWithNoStale('status=assignment-approved')
        self.assertEqual(respond[1], 200, "put request status change")
        self.assertEqual(self.resultLength(respond), 1)
        
        # assign with team
        # test put request with just status change
        data = {'RequestStatus': 'assigned'}
        data.update(self.rerecoAssignArgs)
        self.putRequestWithAuth(requestName, data)
        respond = self.getRequestWithNoStale('status=assigned')
        self.assertEqual(respond[1], 200, "put request status change")
        self.assertEqual(self.resultLength(respond), 1)
        
        respond = self.getRequestWithNoStale('status=assigned&team=%s' % 
                                             self.rerecoAssignArgs['Team'])
        self.assertEqual(respond[1], 200, "put request status change")
        self.assertEqual(self.resultLength(respond), 1)
        
        respond = self.cloneRequestWithAuth(requestName)
        self.assertEqual(respond[1], 200, "put request clone")
        respond = self.getRequestWithNoStale('status=new')
        self.assertEqual(self.resultLength(respond), 1)
        
    def atestRequestClone(self):
        requestName = self.insertRequest(self.rerecoCreateArgs)
        respond = self.cloneRequestWithAuth(requestName)
        print respond
        self.assertEqual(respond[1], 200, "put request clone")
        respond = self.getRequestWithNoStale('status=new')
        self.assertEqual(self.resultLength(respond), 2)
    
if __name__ == '__main__':
    unittest.main()
