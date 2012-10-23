#!/usr/bin/env python

"""
Admin_t

Test for code in the RequestDB/Admin section
"""
import os
import unittest


from WMCore.Services.Requests import JSONRequests
from WMCore.RequestManager.RequestDB.Interface.Admin import SoftwareManagement
from WMCore.HTTPFrontEnd.RequestManager import ReqMgrWebTools

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMCore_t.RequestManager_t.ReqMgr_t  import RequestManagerConfig
from WMCore_t.RequestManager_t import utils


class AdminTest(RESTBaseUnitTest):
    """
    _AdminTest_

    Test for the lower-level DB code in the admin section
    
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
        reqMgrHost = self.config.getServerUrl()
        self.jsonSender = JSONRequests(reqMgrHost)


    def initialize(self):
        self.config = RequestManagerConfig(
                'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel')
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
    

    def testA_SoftwareManagement(self):
        """
        _SoftwareManagement_

        Test the SoftwareManagement code
        
        """
        self.assertEqual(SoftwareManagement.listSoftware(), {})
        softwareVersions = ReqMgrWebTools.allScramArchsAndVersions()
        ReqMgrWebTools.updateScramArchsAndCMSSWVersions()
        result = SoftwareManagement.listSoftware()
        for scramArch in result.keys():
            self.assertEqual(set(result[scramArch]), set(softwareVersions[scramArch]))

        # Now for each scramArch insert a blank set
        # Because of the way that updateSoftware works, this interprets a blank list
        # as telling it that no softwareVersions are available.
        # It deletes every software version it is not handed, so it should give nothing out.
        for scramArch in result.keys():
            SoftwareManagement.updateSoftware(softwareNames = [], scramArch = scramArch)
        self.assertEqual(SoftwareManagement.listSoftware(), {})

        # import has to be here, otherwise getting:
        # AttributeError: 'Toolbox' object has no attribute 'secmodv2' from the Admin module
        from WMCore.HTTPFrontEnd.RequestManager import Admin
        setattr(self.config, 'database', self.testInit.coreConfig.CoreDatabase)
        self.config.section_('templates')
        self.config.section_('html')
        admin = Admin.Admin(self.config)

        ReqMgrWebTools.updateScramArchsAndCMSSWVersions()
        self.assertTrue('slc5_amd64_gcc434' in admin.scramArchs())
        
        

if __name__=='__main__':
    unittest.main()