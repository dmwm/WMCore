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
import cherrypy
from httplib import HTTPException

from WMCore.Services.Requests import JSONRequests
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper, WMWorkload
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMCore.WebTools.FrontEndAuth import FrontEndAuth, NullAuth

from WMCore_t.RequestManager_t.ReqMgr_t import RequestManagerConfig



class AssignTest(RESTBaseUnitTest):
    def setUp(self):
        """
        setUP global values
        Database setUp is done in base class
        
        """
        self.couchDBName = "reqmgr_t_0"
        RESTBaseUnitTest.setUp(self, initRoot = False)
        self.testInit.setupCouch("%s" % self.couchDBName, "ConfigCache", "ReqMgr")
        reqMgrHost = self.config.getServerUrl()
        self.jsonSender = JSONRequests(reqMgrHost)
        

    def initialize(self):
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.Assign')
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


    def loadWorkload(self, requestName):
        """
        _loadWorkload_

        Load the workload from couch after we've saved it there.
        
        """
        workload = WMWorkloadHelper()
        url = '%s/%s/%s/spec' % (os.environ['COUCHURL'], self.couchDBName,
                                      requestName)
        workload.load(url)
        return workload


    def testA_SiteWhitelist(self):
        """
        _SiteWhitelist_

        Test to see if we can get the siteWhitelist to work properly.
        
        """
        secconfig = getattr(self.config, "SecurityModule")
        cherrypy.server.environment = 'test'
        cherrypy.tools.secmodv2 = NullAuth(secconfig)
        
        self.config.UnitTests.views.active.rest.templates    = 'templateDir'
        self.config.UnitTests.views.active.rest.yuiroot      = 'yuiroot'
        self.config.UnitTests.views.active.rest.wildcardKeys = {'T1*': 'T1_*',
                                                                'T2*': 'T2_*',
                                                                'T3*': 'T3_*',
                                                                'US*': '_US_'}
        from WMCore.HTTPFrontEnd.RequestManager.Assign import Assign
        assign = Assign(config = self.config.UnitTests.views.active.rest, noSiteDB = True)

        siteList = ['T1_US_FNAL', 'T1_CH_CERN', 'T1_UK_RAL', 'T2_US_UCSD', 'T2_US_UNL', 'T2_US_CIT']
        self.assertEqual(assign.sites, [])
        assign.sites.extend(siteList)

        Utilities.addSiteWildcards(assign.wildcardKeys,assign.sites,assign.wildcardSites)
        for s in siteList:
            self.assertTrue(s in assign.sites)
        self.assertTrue('T1*' in assign.sites)
        self.assertTrue('T2*' in assign.sites)
        self.assertFalse('T3*' in assign.sites)
        self.assertTrue('US*' in assign.sites)

        self.assertEqual(assign.wildcardSites['T1*'], ['T1_US_FNAL', 'T1_CH_CERN', 'T1_UK_RAL'])
        self.assertEqual(assign.wildcardSites['T2*'], ['T2_US_UCSD', 'T2_US_UNL', 'T2_US_CIT'])
        self.assertEqual(assign.wildcardSites['US*'], ['T1_US_FNAL', 'T2_US_UCSD', 'T2_US_UNL', 'T2_US_CIT'])


if __name__=='__main__':
    unittest.main()