import os
import unittest
import shutil

from WMCore_t.ReqMgr_t.TestConfig import config
from WMQuality.REST.RESTBaseUnitTestWithCouchDB import RESTBaseUnitTestWithCouchDB



class AuxiliaryTest(RESTBaseUnitTestWithCouchDB):
    def setUp(self):
        self.setConfig(config)
        self.setCouchDBs([(config.views.data.couch_reqmgr_db, "ReqMgr")])
        self.setSchemaModules([])
        RESTBaseUnitTestWithCouchDB.setUp(self)
        # without this header getting decompression error
        self.headers = {"Accept": "application/json",
                        "accept-encoding": "gzip,identity"}
        
        
    def tearDown(self):
        RESTBaseUnitTestWithCouchDB.tearDown(self)

    
    def test_A_HelloWorld_get(self):
        r = self.jsonSender.get("data/hello", incoming_headers=self.headers)
        self.assertEqual(r[0]["result"][0], "Hello world")
        r = self.jsonSender.get("data/hello?name=Tiger",
                                incoming_headers=self.headers)
        self.assertEqual(r[0]["result"][0], "Hello Tiger")
        
    
    def test_B_Info_get(self):
        # continue with this class from Auxiliary
        pass
        
        
        
if __name__ == "__main__":
    unittest.main()