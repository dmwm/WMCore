#!/usr/bin/env python
"""
Test case for SiteDB
"""
__revision__ = "$Id: SiteDB_t.py,v 1.9 2010/03/22 12:24:56 swakef Exp $"
__version__  = "$Revision: 1.9 $"
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
        self.failUnless(sorted(results) == sorted(target))


    def testPhEDExNodetocmsName(self):
        """
        Tests PhEDExNodetocmsName
        """
        result = self.mySiteDB.phEDExNodetocmsName('T1_US_FNAL_MSS')
        self.failUnless(result == 'T1_US_FNAL')
        result = self.mySiteDB.phEDExNodetocmsName('T1_US_FNAL_Buffer')
        self.failUnless(result == 'T1_US_FNAL')
        result = self.mySiteDB.phEDExNodetocmsName('T2_UK_London_IC')
        self.failUnless(result == 'T2_UK_London_IC')
        self.assertRaises(ValueError, self.mySiteDB.phEDExNodetocmsName,
                          'T9_DOESNT_EXIST_Buffer')


    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = ['srm-cms.gridpp.rl.ac.uk']
        results = self.mySiteDB.cmsNametoSE("T1_UK_RAL")
        self.failUnless(sorted(results) == sorted(target))


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
        target = ['lcgce06.gridpp.rl.ac.uk', 'lcgce07.gridpp.rl.ac.uk']

        results = self.mySiteDB.cmsNametoCE("T1_UK_RAL")

        self.failUnless(sorted(results) == target)


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
