#!/usr/bin/env python

from __future__ import print_function

import json
import os
import unittest
from http.client import HTTPException

from WMCore_t.ReqMgr_t.TestConfig import config

from WMCore.REST.Test import fake_authz_headers
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend

ADMIN = {'role': ["admin_role"], 'group': ["admin_group"]}
OPS = {'role': ["ops_role"], 'group': ["ops_group"]}
PPD = {'role': ["ppd_role"], 'group': ["ppd_group"]}


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

    return fake_authz_headers(hmacData, dn="/TEST/DN/CN", roles=roles, format="dict")


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
        # Warning: this assumes the same structure in jenkins wmcore_root/test
        self.admin_header = getAuthHeader(self.test_authz_key.data, ADMIN)
        self.ops_header = getAuthHeader(self.test_authz_key.data, OPS)
        self.ppd_header = getAuthHeader(self.test_authz_key.data, PPD)

    def setUp(self, initRoot=True):
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr"),
                          (config.views.data.couch_reqmgr_aux_db, None)])
        self.setSchemaModules([])

        super().setUp(initRoot=initRoot)

        self.setFakeDN()
        # print "%s" % self.test_authz_key.data
        # self.default_status_header = getAuthHeader(self.test_authz_key.data, PPD)

        normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
        rerecoPath = os.path.join(normPath, 'data/ReqMgr/requests/DMWM/ReReco_RunBlockWhite.json')
        with open(rerecoPath, encoding='utf-8') as jObj:
            rerecoArgs = json.load(jObj)
        self.rerecoCreateArgs = rerecoArgs["createRequest"]
        self.rerecoCreateArgs["PrepID"] = "test_prepid"
        self.rerecoAssignArgs = rerecoArgs["assignRequest"]
        # overwrite rereco args
        self.rerecoAssignArgs["AcquisitionEra"] = "test_aqc"
        self.rerecoAssignArgs["Dashboard"] = "test"
        self.rerecoAssignArgs["ProcessingString"] = "ProcStr_ReReco"
        self.rerecoAssignArgs["SiteWhitelist"] = ["T2_CH_CERN"]

        taskChainPath = os.path.join(normPath, 'data/ReqMgr/requests/DMWM/TC_LHE_PFN.json')
        with open(taskChainPath, encoding='utf-8') as jObj:
            lheArgs = json.load(jObj)
        self.lheStep0CreateArgs = lheArgs["createRequest"]
        self.lheStep0AssignArgs = lheArgs["assignRequest"]
        self.lheStep0AssignArgs["AcquisitionEra"] = "test_aqc"

        cmsswDoc = {"_id": "software"}
        cmsswDoc[self.rerecoCreateArgs["ScramArch"]] = []
        cmsswDoc[self.rerecoCreateArgs["ScramArch"]].append(self.rerecoCreateArgs["CMSSWVersion"])
        insertDataToCouch(os.getenv("COUCHURL"), config.views.data.couch_reqmgr_aux_db, cmsswDoc)

    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)

    def getRequestWithNoStale(self, query):
        prefixWithNoStale = "data/request?_nostale=true&"
        return self.jsonSender.get(prefixWithNoStale + query,
                                   incoming_headers=self.ppd_header)

    def postRequestWithAuth(self, data):
        return self.jsonSender.post('data/request', data, incoming_headers=self.ppd_header)

    def putRequestWithAuth(self, requestName, data, clientHeaders):
        """
        WMCore.REST doesn take query for the put request.
        data need to send on the body
        """
        return self.jsonSender.put('data/request/%s' % requestName, data,
                                   incoming_headers=clientHeaders)

    def getMultiRequestsWithAuth(self, data):
        return self.jsonSender.post('data/request/bynames', data, incoming_headers=self.ops_header)

    def cloneRequestWithAuth(self, requestName, params=None):
        """
        WMCore.REST doesn take query for the put request.
        data need to send on the body
        """
        params = params or {}
        # params["OriginalRequestName"] = requestName
        return self.jsonSender.put('data/request/clone/%s' % requestName, params,
                                   incoming_headers=self.ops_header)

    def resultLength(self, response):
        # e.g. of response object: ({'result': []}, 200, 'OK', False)
        return len(response[0]['result'])

    def insertRequest(self, args):
        # test post method
        response = self.postRequestWithAuth(self.rerecoCreateArgs)
        self.assertEqual(response[1], 200)
        requestName = response[0]['result'][0]['request']
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
        response = self.getRequestWithNoStale('name=%s' % requestName)
        self.assertEqual(response[1], 200, "get by name")
        self.assertEqual(self.resultLength(response), 1)

        # get by status
        response = self.getRequestWithNoStale('status=new')
        self.assertEqual(response[1], 200, "get by status")
        self.assertEqual(self.resultLength(response), 1)

        # get by prepID
        response = self.getRequestWithNoStale('prep_id=%s' % self.rerecoCreateArgs["PrepID"])
        self.assertEqual(response[1], 200)
        self.assertEqual(self.resultLength(response), 1)
        # import pdb
        # pdb.set_trace()
        response = self.getRequestWithNoStale('campaign=%s' % self.rerecoCreateArgs["Campaign"])
        self.assertEqual(response[1], 200)
        self.assertEqual(self.resultLength(response), 1)

        response = self.getRequestWithNoStale('inputdataset=%s' % self.rerecoCreateArgs["InputDataset"])
        self.assertEqual(response[1], 200)
        self.assertEqual(self.resultLength(response), 1)

        # test put request with just status change
        data = {'RequestStatus': 'assignment-approved'}
        self.putRequestWithAuth(requestName, data, self.ppd_header)
        response = self.getRequestWithNoStale('status=assignment-approved')
        self.assertEqual(response[1], 200, "put request status change")
        self.assertEqual(self.resultLength(response), 1)

        # PPD should not be able to assign requests
        data = {'RequestStatus': 'assigned'}
        data.update(self.rerecoAssignArgs)
        with self.assertRaises(HTTPException):
            self.putRequestWithAuth(requestName, data, self.ppd_header)
        response = self.getRequestWithNoStale('status=assigned')
        self.assertEqual(response[1], 200, "put request status change")
        print(response)
        self.assertEqual(self.resultLength(response), 0)

        # now try the same transition, but with Ops credentials
        self.putRequestWithAuth(requestName, data, self.ops_header)
        response = self.getRequestWithNoStale('status=assigned')
        self.assertEqual(response[1], 200, "put request status change")
        self.assertEqual(self.resultLength(response), 1)

        response = self.getRequestWithNoStale('status=assigned&team=%s' %
                                              self.rerecoAssignArgs['Team'])
        self.assertEqual(response[1], 200, "put request status change")
        self.assertEqual(self.resultLength(response), 1)

        response = self.getMultiRequestsWithAuth([requestName])
        self.assertEqual(self.resultLength(response), 1)
        self.assertEqual(list(response[0]['result'][0])[0], requestName)

    def atestRequestCombinedGetCall(self):
        """
        test request composite get call
        """
        rerecoReqName = self.insertRequest(self.rerecoCreateArgs)
        lheReqName = self.insertRequest(self.lheStep0CreateArgs)

        ## test get method and get by name
        response = self.getRequestWithNoStale('name=%s&name=%s' % (rerecoReqName, lheReqName))
        self.assertEqual(response[1], 200, "get by name")
        self.assertEqual(self.resultLength(response), 2)

        # get by status
        response = self.getRequestWithNoStale('status=new')
        self.assertEqual(response[1], 200, "get by status")
        self.assertEqual(self.resultLength(response), 2)

        # get by prepID
        response = self.getRequestWithNoStale('prep_id=%s' % self.rerecoCreateArgs["PrepID"])
        self.assertEqual(response[1], 200)
        self.assertEqual(self.resultLength(response), 2)
        # import pdb
        # pdb.set_trace()
        response = self.getRequestWithNoStale('campaign=%s' % self.rerecoCreateArgs["Campaign"])
        self.assertEqual(response[1], 200)
        self.assertEqual(self.resultLength(response), 2)

    def atestRequestClone(self):
        requestName = self.insertRequest(self.rerecoCreateArgs)
        response = self.cloneRequestWithAuth(requestName)
        print(response)
        self.assertEqual(response[1], 200, "put request clone")
        response = self.getRequestWithNoStale('status=new')
        self.assertEqual(self.resultLength(response), 2)


if __name__ == '__main__':
    unittest.main()
