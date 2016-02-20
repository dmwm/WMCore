import unittest
import WMCore

from WMCore_t.ReqMgr_t.TestConfig import config
from WMQuality.REST.RESTBaseUnitTestWithDBBackend import RESTBaseUnitTestWithDBBackend


class AuxiliaryTest(RESTBaseUnitTestWithDBBackend):
    def setUp(self):
        config.main.tools.cms_auth.policy = "dangerously_insecure"
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr")])
        self.setSchemaModules([])
        RESTBaseUnitTestWithDBBackend.setUp(self)        
        
    def tearDown(self):
        RESTBaseUnitTestWithDBBackend.tearDown(self)        
    
    def test_B_Info_get(self):
        r = self.jsonSender.get("data/info")
        self.assertEqual(r[0]["result"][0]['wmcore_reqmgr_version'], WMCore.__version__)

        
        
        
if __name__ == "__main__":
    unittest.main()