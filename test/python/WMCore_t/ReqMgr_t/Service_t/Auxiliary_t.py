from __future__ import print_function

from builtins import range
from future import standard_library
standard_library.install_aliases()

import unittest
import time
from http.client import HTTPException

from WMCore_t.ReqMgr_t.TestConfig import config
from nose.plugins.attrib import attr

from Utils.PythonVersion import PY3

import WMCore
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend


class AuxiliaryTest(RESTBaseUnitTestWithDBBackend):
    def setUp(self):
        config.main.tools.cms_auth.policy = "dangerously_insecure"
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr"),
                          (config.views.data.couch_reqmgr_aux_db, "ReqMgrAux")])
        self.setSchemaModules([])
        RESTBaseUnitTestWithDBBackend.setUp(self)
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)

    def testProcStatus(self):
        """Test the `proc_status` REST API"""
        res = self.jsonSender.get("data/proc_status")
        self.assertItemsEqual(list(res[0]["result"][0]), ['server'])

    def testInfo(self):
        """Test the `info` REST API"""
        res = self.jsonSender.get("data/info")
        self.assertEqual(res[0]["result"][0]['wmcore_reqmgr_version'], WMCore.__version__)

    def testAbout(self):
        """Test the `about` REST API"""
        res = self.jsonSender.get("data/about")
        self.assertEqual(res[0]["result"][0]['wmcore_reqmgr_version'], WMCore.__version__)

    def testStatus(self):
        """Test the `status` REST API"""
        res = self.jsonSender.get("data/status")
        self.assertTrue(len(res[0]['result']) == 19)
        for st in {"aborted-archived", "new", "completed"}:
            self.assertIn(st, res[0]['result'])

    def testType(self):
        """Test the `type` REST API"""
        res = self.jsonSender.get("data/type")
        self.assertItemsEqual(res[0]["result"],
                              ['ReReco', 'StoreResults', 'Resubmission', 'TaskChain', 'DQMHarvest', 'StepChain'])

    def testPermissions(self):
        """Test the `permissions` REST API"""
        # no document yet available
        with self.assertRaises(HTTPException):
            self.jsonSender.get("data/permissions")

    def testCMSSWVersions(self):
        """Test the `cmsswversions` REST API"""
        myDoc = {"slc6": ["CMS1", "CMS2", "CMS3"], "slc7": ["CMS3", "CMS4"]}
        res = self.jsonSender.post("data/cmsswversions", myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/cmsswversions")
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "CMSSW_VERSIONS")
        self.assertItemsEqual(list(res[0]["result"][0]), ["slc6", "slc7", "ConfigType"])

    def testPutCMSSWVersions(self):
        """Test the `cmsswversions` REST API with PUT call"""
        myDoc = {"slc6": ["CMS1", "CMS2", "CMS3"]}
        res = self.jsonSender.put("data/cmsswversions", myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/cmsswversions")
        self.assertItemsEqual(list(res[0]["result"][0]), ["slc6", "ConfigType"])

    def testWMAgentConfig(self):
        """Test the `wmagentconfig` REST API"""
        docName = "wmagent1"
        myDoc = {"key1": "blah"}
        res = self.jsonSender.post("data/wmagentconfig/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/wmagentconfig/%s" % docName)
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "WMAGENT_CONFIG")
        self.assertItemsEqual(list(res[0]["result"][0]), ["key1", "ConfigType"])

    @attr("integration")
    def testPutWMAgentConfig(self):
        """
        Same as testWMAgentConfig, but test the PUT call in a different unit
        test because jenkins are not happy to run those
        """
        self.testWMAgentConfig()

        # now change the document with a PUT call
        docName = "wmagent1"
        myDoc = {"key2": ["blah"], "nono": False}
        res = self.jsonSender.put("data/wmagentconfig/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/wmagentconfig/%s" % docName)
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "WMAGENT_CONFIG")
        self.assertItemsEqual(list(res[0]["result"][0]), ["key2", "nono", "ConfigType"])

    def testCampaignConfig(self):
        """Test the `campaignconfig` REST API"""
        docName = "camp1"
        myDoc = {"campName": "Camp1", "keyblah": False}
        res = self.jsonSender.post("data/campaignconfig/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/campaignconfig/%s" % docName)
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "CAMPAIGN_CONFIG")
        self.assertItemsEqual(list(res[0]["result"][0]), ["campName", "keyblah", "ConfigType"])

    @attr("integration")
    def testPutCampaignConfig(self):
        """
        Same as testCampaignConfig, but test the PUT call in a different unit
        test because jenkins are not happy to run those
        """
        self.testCampaignConfig()

        # now change the document with a PUT call
        docName = "camp1"
        myDoc = {"campName": "NewCamp1", "keyblah": True}
        res = self.jsonSender.put("data/campaignconfig/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/campaignconfig/%s" % docName)
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "CAMPAIGN_CONFIG")
        self.assertItemsEqual(list(res[0]["result"][0]), ["campName", "keyblah", "ConfigType"])

    def testUnifiedConfig(self):
        """Test the `unifiedconfig` REST API"""
        docName = "uni1"
        myDoc = {"key1": "value1"}
        res = self.jsonSender.post("data/unifiedconfig/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/unifiedconfig/%s" % docName)
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "UNIFIED_CONFIG")
        self.assertItemsEqual(list(res[0]["result"][0]), ["key1", "ConfigType"])

    @attr("integration")
    def testPutUnifiedConfig(self):
        """
        Same as testUnifiedConfig, but test the PUT call in a different unit
        test because jenkins are not happy to run those
        """
        self.testUnifiedConfig()

        # now change the document with a PUT call
        docName = "uni1"
        myDoc = {"key2": ["blah"], "key3": True}
        res = self.jsonSender.put("data/unifiedconfig/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/unifiedconfig/%s" % docName)
        self.assertItemsEqual(list(res[0]["result"][0]), ["key2", "key3", "ConfigType"])

    def testTransferInfo(self):
        """Test the `transferinfo` REST API"""
        docName = "trans1"
        myDoc = {"key1": "value1"}
        res = self.jsonSender.post("data/transferinfo/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/transferinfo/%s" % docName)
        self.assertTrue(res[0]["result"][0]["ConfigType"] == "TRANSFER")
        self.assertItemsEqual(list(res[0]["result"][0]), ["key1", "ConfigType"])

    @attr("integration")
    def testPutTransferInfo(self):
        """
        Same as testTransferInfo, but test the PUT call in a different unit
        test because jenkins are not happy to run those
        """
        self.testTransferInfo()

        # now change the document with a PUT call
        docName = "trans1"
        myDoc = {"key2": ["blah"], "key3": True}
        res = self.jsonSender.put("data/transferinfo/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/transferinfo/%s" % docName)
        self.assertItemsEqual(list(res[0]["result"][0]), ["key2", "key3", "ConfigType"])

    def testAllTransferDocs(self):
        """Test the `transferinfo` REST API"""
        self.testTransferInfo()
        ### views not built/indexed yet
        res = self.jsonSender.get("data/transferinfo/ALL_DOCS")
        res = self.jsonSender.get("data/transferinfo/ALL_DOCS")
        if not res[0]["result"]:
            res = self.jsonSender.get("data/transferinfo/ALL_DOCS")
        self.assertEqual(len(res[0]["result"]), 1)

        docName = "trans2"
        myDoc = {"key_transf2": "transfer2"}
        res = self.jsonSender.post("data/transferinfo/%s" % docName, myDoc)
        self.assertTrue(res)

        res = self.jsonSender.get("data/transferinfo/ALL_DOCS")  # views still to be updated...
        self.assertEqual(len(res[0]["result"]), 1)

        # trick to make sure views have been updated
        for i in range(3):
            try:
                res = self.jsonSender.get("data/transferinfo/ALL_DOCS")
                self.assertEqual(len(res[0]["result"]), 2)
            except AssertionError:
                if i == 2:
                    raise
                else:
                    print("waiting 1 second for views to be updated...")
                    time.sleep(1)


if __name__ == "__main__":
    unittest.main()
