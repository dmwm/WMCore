#!/usr/bin/env python
"""
_ReDigi_t_

Unit tests for the ReDigi workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.ReDigi import getTestArguments, reDigiWorkload

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document

class ReDigiTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("redigi_t", "ConfigCache")        
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("redigi_t")
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.        
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return

    def injectReDigiConfigs(self):
        """
        _injectReDigiConfigs_

        Create a bogus config cache documents for the various steps of the
        ReDigi workflow.  Return the IDs of the documents.
        """
        stepOneConfig = Document()
        stepOneConfig["info"] = None
        stepOneConfig["config"] = None
        stepOneConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        stepOneConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        stepOneConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        stepOneConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RAWDEBUGoutput"],
                                                          "RAWDEBUGoutput": {"dataset": {"filterName": "",
                                                                                         "dataTier": "RAW-DEBUG-OUTPUT"}}}}

        stepTwoConfig = Document()
        stepTwoConfig["info"] = None
        stepTwoConfig["config"] = None
        stepTwoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        stepTwoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        stepTwoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        stepTwoConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RECODEBUGoutput", "DQMoutput"],
                                                          "RECODEBUGoutput": {"dataset": {"filterName": "",
                                                                                          "dataTier": "RECO-DEBUG-OUTPUT"}},
                                                          "DQMoutput": {"dataset": {"filterName": "",
                                                                                    "dataTier": "DQM"}}}}

        stepThreeConfig = Document()
        stepThreeConfig["info"] = None
        stepThreeConfig["config"] = None
        stepThreeConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        stepThreeConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        stepThreeConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        stepThreeConfig["pset_tweak_details"] ={"process": {"outputModules_": ["aodOutputModule"],
                                                            "aodOutputModule": {"dataset": {"filterName": "",
                                                                                            "dataTier": "AODSIM"}}}}        
        stepOne = self.configDatabase.commitOne(stepOneConfig)[0]["id"]
        stepTwo = self.configDatabase.commitOne(stepTwoConfig)[0]["id"]
        stepThree = self.configDatabase.commitOne(stepThreeConfig)[0]["id"]        
        return (stepOne, stepTwo, stepThree)
    
    def testReDigi(self):
        """
        _testReDigi_
        
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "redigi_t"
        configs = self.injectReDigiConfigs()
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")
        
        testWMBSHelper = WMBSHelper(testWorkload, "SomeBlock")
        testWMBSHelper.createSubscription()

        return

if __name__ == '__main__':
    unittest.main()
