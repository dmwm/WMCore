#!/usr/bin/env python
"""
Test case for SiteDB
"""
__revision__ = "$Id: SiteDB_t.py,v 1.8 2009/10/13 22:43:00 meloam Exp $"
__version__  = "$Revision: 1.8 $"
__author__   = "ewv@fnal.gov"

import unittest

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

class SiteDBTest(unittest.TestCase):
    """
    Unit tests for SiteScreening module
    """


    def setUp(self):
        """
        Setup for unit tests
        """
        self.mySiteDB = SiteDBJSON()




    def testCmsNametoPhEDExNode(self):
        """
        Tests CmsNametoSE
        """
        target = ['T1_US_FNAL_MSS','T1_US_FNAL_Buffer']
        results = self.mySiteDB.cmsNametoPhEDExNode("T1_US_FNAL")
        self.failUnless(results.sort() == target.sort())


    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = ['srm-cms.gridpp.rl.ac.uk','ralsrma.rl.ac.uk',
                  'ralsrmf.rl.ac.uk','ralsrme.rl.ac.uk','ralsrmb.rl.ac.uk']
        results = self.mySiteDB.cmsNametoSE("T1_UK_RAL")
        self.failUnless(results.sort() == target.sort())


    def testSEtoCmsName(self):
        """
        Tests CmsNametoSE
        """
        target = 'T1_US_FNAL'
        results = self.mySiteDB.seToCMSName("cmssrm.fnal.gov")
        self.failUnless(results == target)


    def testCmsNametoCE(self):
        """
        Tests CmsNametoCE
        """
        target = ['lcgce02.gridpp.rl.ac.uk']

        results = self.mySiteDB.cmsNametoCE("T1_UK_RAL")

        self.failUnless(results == target)


    def testJSONParser(self):
        """
        Tests the JSON parser directly
        """
        cmsName = "cmsgrid02.hep.wisc.edu"
        results = self.mySiteDB.getJSON("CEtoCMSName",
                                  file="CEtoCMSName",
                                  name=cmsName)
        self.failUnless(results['0']['name'] == "T2_US_Wisconsin")


    def testDNUserName(self):
        """
        Tests DN to Username lookup
        """

        testDn       = "/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson"
        testUserName = "metson"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.failUnless(testUserName == userName)



if __name__ == '__main__':
    unittest.main()
