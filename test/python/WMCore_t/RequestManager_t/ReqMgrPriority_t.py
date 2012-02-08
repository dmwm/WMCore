#!/usr/bin/env python

"""
RequestManager unittest

Tests the functions of the REST API
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

import WMCore.WMSpec.StdSpecs.ReReco as ReReco

from WMCore_t.RequestManager_t.ReqMgr_t import RequestManagerConfig, getRequestSchema

class ReqMgrPriorityTest(RESTBaseUnitTest):
    """
    _ReqMgrPriorityTest_

    Basic test for setting the priority in ReqMgr Services
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
        self.config.setPort(8888)
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

    def setupSchema(self, groupName = 'PeopleLikeMe',
                    userName = 'me', teamName = 'White Sox',
                    CMSSWVersion = 'CMSSW_3_5_8',
                    scramArch = 'slc5_ia32_gcc434'):
        """
        _setupSchema_

        Set up a test schema so that we can run a test request.
        Standardization!
        """

        self.jsonSender.put('user/%s?email=me@my.com' % userName)
        self.jsonSender.put('group/%s' % groupName)
        self.jsonSender.put('group/%s/%s' % (groupName, userName))
        self.jsonSender.put(urllib.quote('team/%s' % teamName))
        self.jsonSender.put('version/%s/%s' % (CMSSWVersion, scramArch))

        schema = ReReco.getTestArguments()
        schema['RequestName'] = 'TestReReco'
        schema['RequestType'] = 'ReReco'
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

    def changeStatusAndCheck(self, requestName, statusName):
        """
        _changeStatusAndCheck_

        Change the status of a request and make sure that
        the request actually did it.
        """
        self.jsonSender.put('request/%s?status=%s' % (requestName, statusName))
        result = self.jsonSender.get('request/%s' % requestName)
        self.assertEqual(result[0]['RequestStatus'], statusName)
        return


    def testA_RequestPriority(self):
        """
        _priorityChanges_

        Do some fairly standard priority changes to the Request and
        see how things react
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion)

        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), 0)

        # Reset user, group priorities to 0
        self.jsonSender.post('user/%s?priority=0' % userName)
        self.jsonSender.post('group/%s?priority=0' % groupName)

        # Set priority == 5
        priority = 5
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)

        # Set priority == 100
        priority = 100
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)

        # Set priority == -1
        priority = -1
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)

        # Let's move the request around a bit
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'testing-approved')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'testing')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'tested')
        self.changeStatusAndCheck(requestName = requestName,
                                  statusName  = 'assignment-approved')
        self.jsonSender.put(urllib.quote('assignment/%s/%s' % (teamName, requestName)))

        # Set priority == 99
        priority = 99
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)
        return

    def testB_InvalidPriority(self):
        """
        _InvalidPriority_

        Put in a bunch of invalid values for priorities and
        see what the code makes of them.
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion)

        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        raises = False
        try:
            priority = sys.maxint + 1
            self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            self.assertTrue("Priority must have abs() less then MAXINT!" in ex.result)
        self.assertTrue(raises)

        raises = False
        try:
            priority = -1 - sys.maxint
            self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            print ex.result
            self.assertTrue("Priority must have abs() less then MAXINT!" in ex.result)
        self.assertTrue(raises)

        # Now try to violate the limits put on the requestPriority
        raises = False
        try:
            priority = 9999
            self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        except HTTPException, ex:
            raises = True
            self.assertEqual(ex.status, 400)
            print ex.result
            self.assertTrue("Request priority must have abs() less then 100" in ex.result)
        self.assertTrue(raises)

        return


    def testC_UserGroupRequestPriority(self):
        """
        _UserGroupRequestPriority_

        Set the priorities of the user, the group, and the request
        """

        userName     = 'Taizong'
        groupName    = 'Li'
        teamName     = 'Tang'
        CMSSWVersion = 'CMSSW_3_5_8'
        schema       = self.setupSchema(userName = userName,
                                        groupName = groupName,
                                        teamName = teamName,
                                        CMSSWVersion = CMSSWVersion)

        result = self.jsonSender.put('request/testRequest', schema)
        self.assertEqual(result[1], 200)
        requestName = result[0]['RequestName']

        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), 0)

        # Reset user, group priorities to 0
        self.jsonSender.post('user/%s?priority=0' % userName)
        self.jsonSender.post('group/%s?priority=0' % groupName)

        # Set priority == 5
        priority = 5
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)

        self.jsonSender.post('user/%s?priority=6' % userName)
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority + 6)

        self.jsonSender.post('group/%s?priority=7' % groupName)
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority + 6 + 7)

        self.jsonSender.post('user/%s?priority=0' % userName)
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['ReqMgrRequestBasePriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority + 7)

        return



if __name__=='__main__':
    unittest.main()
