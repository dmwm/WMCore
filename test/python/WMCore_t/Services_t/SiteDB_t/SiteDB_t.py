#!/usr/bin/env python
"""
Test case for SiteDB
"""

import unittest

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from nose.plugins.attrib import attr

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
        # don't check this anymore, see comment in phEDExNodetocmsName function
        #self.assertRaises(ValueError, self.mySiteDB.phEDExNodetocmsName,
        #                  'T9_DOESNT_EXIST_Buffer')

    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = ['srm-cms.gridpp.rl.ac.uk', 'srm-cms-disk.gridpp.rl.ac.uk']
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
        target = ['lcgce09.gridpp.rl.ac.uk', 'lcgce06.gridpp.rl.ac.uk',
                  'lcgce07.gridpp.rl.ac.uk', 'lcgce07.gridpp.rl.ac.uk']
        results = self.mySiteDB.cmsNametoCE("T1_UK_RAL")
        self.failUnless(sorted(results) == sorted(target))

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
        testDn = "/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson"
        testUserName = "metson"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.failUnless(testUserName == userName)

    @attr("integration")
    def testDNWithApostrophe(self):
        """
        Tests a DN with an apostrophy in - will fail till SiteDB2 appears
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'"
        testUserName = "liviof"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.failUnless(testUserName == userName)

    def testSEFinder(self):
        """
        _testSEFinder_

        See if we can retrieve seNames from all sites
        """

        ceNames = self.mySiteDB.getAllSENames()
        self.assertTrue(len(ceNames) > 1)
        self.assertTrue('cmssrm.fnal.gov' in ceNames)
        return


    @attr("integration")
    def testParsingJsonWithApostrophe(self):
        """
        Tests parsing a DN json with an apostrophe in
        """
        json = """{"dn": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'", "user": "liviof"}"""
        d = self.mySiteDB.parser.dictParser(json)
        self.assertEquals("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'", d['dn'])

    @attr("integration")
    def testParsingInvalidJsonWithApostrophe(self):
        """
        Tests parsing a DN invalid json (from sitedb v1) with an apostrophe in
        """
        json = """{'dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio' Fano', 'user': 'liviof'}"""
        d = self.mySiteDB.parser.dictParser(json)
        self.assertEquals("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio' Fano", d['dn'])
        json = """{'dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'', 'user': 'liviof'}"""
        d = self.mySiteDB.parser.dictParser(json)
        self.assertEquals("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'", d['dn'])

if __name__ == '__main__':
    unittest.main()
