#!/usr/bin/env python

from __future__ import print_function

import json
import os
import unittest

from WMCore_t.ReqMgr_t.TestConfig import config

from WMCore.REST.Test import fake_authz_headers
from WMCore.ReqMgr.Auth import getWritePermission
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend

req_args = {"RequestType": "ReReco", "RequestStatus": None}
ADMIN_PERMISSION = getWritePermission(req_args)

req_args = {"RequestType": "ReReco", "RequestStatus": "completed"}
DEFAULT_STATUS_PERMISSION = getWritePermission(req_args)

req_args = {"RequestType": "ReReco", "RequestStatus": "new"}
CREATE_PERMISSION = getWritePermission(req_args)

DEFAULT_PERMISSION = DEFAULT_STATUS_PERMISSION

req_args = {"RequestType": "ReReco", "RequestStatus": "assinged"}
ASSIGN_PERMISSION = getWritePermission(req_args)


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

        normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
        rerecoPath = os.path.join(normPath, 'data/ReqMgr/requests/DMWM/ReReco_RunBlockWhite.json')
        with open(rerecoPath) as jObj:
            rerecoArgs = json.load(jObj)

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
        self.assertEqual(len(response), 1)
        requestName = response[0]['request']

        ## test get method
        # get by name
        response = self.reqSvc.getRequestByNames(requestName)
        self.assertEqual(response[0][requestName]['RequestPriority'], 190000)
        self.assertEqual(len(response), 1)

        # get by status
        response = self.reqSvc.getRequestByStatus('new')
        self.assertEqual(len(response), 1)

        self.reqSvc.updateRequestStatus(requestName, 'assignment-approved')
        response = self.reqSvc.getRequestByStatus('assignment-approved')
        self.assertEqual(len(response), 1)

        self.reqSvc.updateRequestProperty(requestName, {'RequestStatus': 'assigned',
                                                        "AcquisitionEra": "TEST_ERA",
                                                        "Team": "unittest",
                                                        "SiteWhitelist": ["T1_US_CBS"],
                                                        "SiteBlacklist": ["T1_US_FOX"]})
        response = self.reqSvc.getRequestByStatus('assignment-approved')
        self.assertEqual(len(response), 0)
        response = self.reqSvc.getRequestByStatus('assigned')
        self.assertEqual(len(response), 1)
        self.assertEqual(list(response[0].values())[0]["SiteWhitelist"], ["T1_US_CBS"])

        self.reqSvc.updateRequestStats(requestName, {'total_jobs': 100, 'input_lumis': 100,
                               'input_events': 100, 'input_num_files': 100})

        response = self.reqSvc.cloneRequest(requestName)
        self.assertEqual(len(response), 1)
        clonedName = response[0]['request']
        response = self.reqSvc.getRequestByNames(clonedName)
        self.assertEqual(response[0][clonedName]['TimePerEvent'], 73.85)

        response = self.reqSvc.cloneRequest(requestName, {'TimePerEvent': 20})
        self.assertEqual(len(response), 1)
        clonedName = response[0]['request']
        response = self.reqSvc.getRequestByNames(clonedName)
        self.assertEqual(response[0][clonedName]['TimePerEvent'], 20)

if __name__ == '__main__':
    unittest.main()
