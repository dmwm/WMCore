#!/usr/bin/env python
"""
Test case for SiteDB
"""
__revision__ = "$Id: SiteDB_t.py,v 1.3 2008/10/16 12:49:56 ewv Exp $"
__version__  = "$Revision: 1.3 $"
__author__   = "ewv@fnal.gov"

import unittest

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

class SiteDBJSONTest(unittest.TestCase):
    """
    Unit tests for SiteScreening module
    """


    def setUp(self):
        """
        Setup for unit tests
        """
        self.mySiteDB = SiteDBJSON()


    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = ['cmssrm.fnal.gov', 'ralsrmb.rl.ac.uk',
                  'gridka-dCache.fzk.de', 'srm-cms.cern.ch',
                  'castorsrm.cr.cnaf.infn.it', 'ccsrm.in2p3.fr',
                  'srm.grid.sinica.edu.tw', 'srm2.grid.sinica.edu.tw',
                  'ralsrma.rl.ac.uk', 'srm-cms.gridpp.rl.ac.uk',
                  'ralsrme.rl.ac.uk', 'ralsrmf.rl.ac.uk']
        results = self.mySiteDB.cmsNametoSE("T1")
        self.failUnless(results == target)


    def testCmsNametoCE(self):
        """
        Tests CmsNametoCE
        """
        target = ['cclcgceli04.in2p3.fr', 'cclcgceli03.in2p3.fr',
                  'w-ce02.grid.sinica.edu.tw', 'w-ce01.grid.sinica.edu.tw',
                  'lcgce02.gridpp.rl.ac.uk', 'lcg00125.grid.sinica.edu.tw']
        results = self.mySiteDB.cmsNametoCE("T1")
        self.failUnless(results == target)


    def testJSONParser(self):
        """
        Tests the JSON parser directly
        """
        cmsName = "red.unl.edu"
        results = self.mySiteDB.parser.getJSON("CEtoCMSName",
                                  file="CEtoCMSName",
                                  name=cmsName)
        self.failUnless(results['0']['name'] == "T2_US_Nebraska")


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
