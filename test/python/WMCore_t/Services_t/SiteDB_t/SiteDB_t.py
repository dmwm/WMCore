#!/usr/bin/env python
"""
Test case for SiteDB
"""
__revision__ = "$Id: SiteDB_t.py,v 1.6 2009/03/31 14:52:02 metson Exp $"
__version__  = "$Revision: 1.6 $"
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

    def runTest(self):
        self.testCmsNametoSE()
        self.testCmsNametoCE()
        self.testJSONParser()
        self.testDNUserName()


    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = ['ralsrmb.rl.ac.uk',
                  'ralsrma.rl.ac.uk', 'srm-cms.gridpp.rl.ac.uk',
                  'ralsrme.rl.ac.uk', 'ralsrmf.rl.ac.uk']
        results = self.mySiteDB.cmsNametoSE("T1_UK_RAL")
        self.failUnless(results.sort() == target.sort())


    def testCmsNametoCE(self):
        """
        Tests CmsNametoCE
        """
        target = ['cclcgceli03.in2p3.fr', 'ce-2-fzk.gridka.de', 
                  'ce-1-fzk.gridka.de', 'cclcgceli04.in2p3.fr', 
                  'ce-4-fzk.gridka.de', 'ce-3-fzk.gridka.de', 'ce06.pic.es', 
                  'ce-5-fzk.gridka.de', 'ce07.pic.es', 
                  'w-ce01.grid.sinica.edu.tw', 'ce04-lcg.cr.cnaf.infn.it', 
                  'lcg00125.grid.sinica.edu.tw', 'w-ce02.grid.sinica.edu.tw', 
                  'ce05-lcg.cr.cnaf.infn.it ', 'ce06-lcg.cr.cnaf.infn.it', 
                  'cmsosgce4.fnal.gov', 'lcgce02.gridpp.rl.ac.uk', 
                  'cmsosgce.fnal.gov', 'cmsosgce2.fnal.gov']

        results = self.mySiteDB.cmsNametoCE("T1")
        
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
