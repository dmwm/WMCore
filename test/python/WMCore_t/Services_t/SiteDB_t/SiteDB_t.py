#!/usr/bin/env python
"""
Test case for SiteDB
"""
__revision__ = "$Id: SiteDB_t.py,v 1.1 2008/10/16 10:56:44 ewv Exp $"
__version__  = "$Revision: 1.1 $"
__author__   = "ewv@fnal.gov"

import unittest
import logging


class SiteDBJSONTest(unittest.TestCase):
    """
    Unit tests for SiteScreening module
    """

    def setUp(self):
        """
        Setup for unit tests
        """
        self.separser = \
            BlackWhiteListParser.SEBlackWhiteListParser(cfgParams, fakeLogger)
        self.ceparser = \
            BlackWhiteListParser.CEBlackWhiteListParser(cfgParams, fakeLogger)

    def testSEBlackList(self):
        """
        Tests black list parsing for Storage Elements
        """
        blacklist = ['ccsrm.in2p3.fr', 'cmssrm.fnal.gov']
        other = ['t2-srm-02.lnl.infn.it', 'se-dcache.hepgrid.uerj.br']
        results = self.separser.checkBlackList(other + blacklist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(other))

    def testSEWhiteList(self):
        """
        Tests white list parsing for Storage Elements
        """
        whitelist = ['srm.ihepa.ufl.edu', 'heplnx204.pp.rl.ac.uk',
            'cluster142.knu.ac.kr']
        other = ['f-dpm001.grid.sinica.edu.tw', 'cmsrm-se01.roma1.infn.it']
        results = self.separser.checkWhiteList(other + whitelist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(whitelist))

    def testCEBlackList(self):
        """
        Tests black list parsing for Compute Elements
        """
        blacklist = ['lcg02.ciemat.es', 'lcgce01.phy.bris.ac.uk',
            'lcgce02.phy.bris.ac.uk', 'cmsosgce4.fnal.gov']
        other = ['osgce.hepgrid.uerj.br', 'egeece01.ifca.es',
            'grid006.lca.uc.pt']
        results = self.ceparser.checkBlackList(other + blacklist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(other))

    def testCEWhiteList(self):
        """
        Tests white list parsing for Compute Elements
        """
        whitelist = ['vampire.accre.vanderbilt.edu',
                     'ic-kit-lcgce.rz.uni-karlsruhe.de']
        other = ['gridce2.pi.infn.it', 'lcg02.ciemat.es']
        results = self.ceparser.checkWhiteList(other + whitelist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(whitelist))


cfgParams = {
  'EDG.se_black_list': 'ccsrm.in2p3.fr, T1_*',
  'EDG.se_white_list': 'srm.ihepa.ufl.edu, heplnx204.pp.rl.ac.uk, cluster142.knu.ac.kr',
  'EDG.ce_black_list': 'lcg02.ciemat.es, bris.ac, *.fnal.gov',
  'EDG.ce_white_list': 'vampire.accre.vanderbilt.edu, ic-kit-lcgce.rz.uni-karlsruhe.de',
}


if __name__ == '__main__':
    unittest.main()
    mySiteDB = SiteDBJSON()

    print "Username for Simon Metson:", \
          mySiteDB.dnUserName(dn="/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson")

    print "CMS name for UNL:", \
          mySiteDB.parser.getJSON("CEtoCMSName",
                                  file="CEtoCMSName",
                                  name="red.unl.edu")

    print "T1 Site Exec's:", \
          mySiteDB.parser.getJSON("CMSNametoAdmins",
                                  file="CMSNametoAdmins",
                                  name="T1",
                                  role="Site Executive")
    print "Tier 1 CEs:", mySiteDB.cmsNametoCE("T1")
    print "Tier 1 SEs:", mySiteDB.cmsNametoSE("T1")
