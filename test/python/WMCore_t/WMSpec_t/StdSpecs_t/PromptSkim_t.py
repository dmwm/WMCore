#!/usr/bin/env python
"""
_PromptSkim_t_

Unit tests for the PromptSkim workflow.
"""

import unittest
import os

from WMCore.WMSpec.StdSpecs.PromptSkim import promptSkimWorkload
from WMCore.WMSpec.StdSpecs.PromptSkim import getTestArguments
from WMCore.WMSpec.StdSpecs.PromptSkim import fixCVSUrl

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document

from nose.plugins.attrib import attr

class PromptSkimTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_
        
        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("promptskim_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("promptskim_t")
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return

    def testFixCVSUrl(self):
        notCVSUrl = 'http://cmsprod.web.cern.ch/cmsprod/oldJobs.html'
        rightCVSUrl = 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_MET.py?revision=1.4'
        wrongCVSUrls = ['http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_MET.py?revision=1.4&view=markup&pathrev=MAIN&sortby=date',
                        'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_MET.py?revision=1.4&view=markup&sortby=author',
                        'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_MET.py?revision=1.4&content-type=text%2Fplain',
                        'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_MET.py?view=markup&revision=1.4&sortby=author']
        self.assertEqual(fixCVSUrl(notCVSUrl), notCVSUrl,
                         'Error: A url not from CVS was changed')
        self.assertEqual(fixCVSUrl(rightCVSUrl), rightCVSUrl,
                         'Error: A correct url from CVS was changed')
        for url in wrongCVSUrls:
            self.assertEqual(fixCVSUrl(url), rightCVSUrl,
                         'Error: A wrong url (%s) from CVS was not changed (%s)'
                         % (url, fixCVSUrl(url)))
    @attr("integration")
    def testPromptSkim(self):
        """
        _testPromptSkim_

        Verify that PromptSkim workflows can be created.  Note that this
        requires a system that has all of the cms software installed on it.
        """
        dataProcArguments = getTestArguments()
        dataProcArguments["CouchUrl"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "promptskim_t"
        testWorkload = promptSkimWorkload("TestWorkload", dataProcArguments)

        #Test another processing version flavor
        dataProcArguments["ProcessingVersion"] = "v1"
        testWorkload = promptSkimWorkload("TestWorkload2", dataProcArguments)
        return

if __name__ == '__main__':
    unittest.main()
