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
from WMCore.WMSpec.StdSpecs.PromptSkim import parseT0ProcVer

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
        """
        _testFixCVSUrl_

        Check that the method identifies broken urls and fixes them,
        also it leaves alone those urls which are not recognized
        """

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

    def testParseT0ProcVer(self):
        """
        _testParseT0Procver_

        Check that the parser function process correctly different
        possibilities of processing versions sent by the T0
        """
        procVerJustNumber = '1'
        procVerWithV = 'v1'
        procVerWithString = 'PromptSkim-v1'
        procVerWrong = 'ProcVer-v1-Wrong'

        result = parseT0ProcVer(procVerJustNumber)
        self.assertEqual(result, {'ProcVer' : 1, 'ProcString' : None})

        result = parseT0ProcVer(procVerWithV)
        self.assertEqual(result, {'ProcVer' : 1, 'ProcString' : None})

        result = parseT0ProcVer(procVerWithV, 'PromptSkim')
        self.assertEqual(result, {'ProcVer' : 1, 'ProcString' : 'PromptSkim'})

        result = parseT0ProcVer(procVerWithString)
        self.assertEqual(result, {'ProcVer' : 1, 'ProcString' : 'PromptSkim'})

        result = parseT0ProcVer(procVerWithString, 'Minor')
        self.assertEqual(result, {'ProcVer' : 1, 'ProcString' : 'PromptSkim'})

        result = parseT0ProcVer(procVerJustNumber, 'TestString')
        self.assertEqual(result, {'ProcVer' : 1, 'ProcString' : 'TestString'})

        self.assertRaises(Exception, parseT0ProcVer, procVerWrong)


    @attr("integration")
    def testPromptSkimA(self):
        """
        _testPromptSkimA_

        Verify that PromptSkim workflows can be created.  Note that this
        requires a system that has all of the cms software installed on it.
        """
        dataProcArguments = getTestArguments()
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "promptskim_t"
        dataProcArguments["EnvPath"] = os.environ.get("EnvPath", None)
        dataProcArguments["BinPath"] = os.environ.get("BinPath", None)
        testWorkload = promptSkimWorkload("TestWorkload", dataProcArguments)

        #Test another processing version flavor
        dataProcArguments["ProcessingVersion"] = "v1"
        testWorkload = promptSkimWorkload("TestWorkload2", dataProcArguments)
        return

    @attr("integration")
    def testPromptSkimB(self):
        """
        _testPromptSkimB_

        Verify that a PromptSkim workflow is not created when a inexistent
        correct url is passed.  Note that this
        requires a system that has all of the cms software installed on it.
        """
        dataProcArguments = getTestArguments()
        dataProcArguments["SkimConfig"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/IAmCorrectButIDontExist?revision=1.4"
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["EnvPath"] = os.environ.get("EnvPath", None)
        dataProcArguments["BinPath"] = os.environ.get("BinPath", None)
        dataProcArguments["CouchDBName"] = "promptskim_t"
        self.assertRaises(Exception, promptSkimWorkload, *["TestWorkload", dataProcArguments])

        return

if __name__ == '__main__':
    unittest.main()
