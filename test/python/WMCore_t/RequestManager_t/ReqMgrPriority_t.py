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
from httplib import HTTPException

from WMCore.Services.Requests import JSONRequests

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

from WMCore_t.RequestManager_t.ReqMgr_t import RequestManagerConfig
from WMCore_t.RequestManager_t import utils


class ReqMgrPriorityTest(RESTBaseUnitTest):
    """
    Basic test for setting the priority in ReqMgr Services
    
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
        self.testInit.setupCouch("%s_acdc" % self.couchDBName, "ACDC")
        reqMgrHost = self.config.getServerUrl()
        self.jsonSender = JSONRequests(reqMgrHost)
        

    def initialize(self):
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
        self.config.setFormatter('WMCore.WebTools.RESTFormatter')
        self.config.setupRequestConfig()
        self.config.setupCouchDatabase(dbName = self.couchDBName)
        self.config.setPort(8888)
        self.schemaModules = ["WMCore.RequestManager.RequestDB"]


    def tearDown(self):
        """
        _tearDown_

        Basic tear down of database
        
        """
        RESTBaseUnitTest.tearDown(self)
        self.testInit.tearDownCouch()
        

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


    def testA_RequestPriority(self):
        """
        _priorityChanges_

        Do some fairly standard priority changes to the Request and
        see how things react
        
        """
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

        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), 0)

        # Set priority == 5
        priority = 5
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['RequestPriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)

        # Set priority == 100
        priority = 100
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['RequestPriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)

        # Set priority == -1
        priority = -1
        self.jsonSender.put('request/%s?priority=%s' % (requestName, priority))
        request = self.jsonSender.get('request/%s' % requestName)[0]
        self.assertEqual(request['RequestPriority'], priority)
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
        self.assertEqual(request['RequestPriority'], priority)
        workload = self.loadWorkload(requestName = requestName)
        self.assertEqual(workload.priority(), priority)
    

    def testB_InvalidPriority(self):
        """
        _InvalidPriority_

        Put in a bunch of invalid values for priorities and
        see what the code makes of them.
        
        """
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


if __name__=='__main__':
    unittest.main()