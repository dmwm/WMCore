#!/usr/bin/env python
"""
Test case for SiteScreening
"""
__revision__ = "$Id: BlackWhiteListParser_t.py,v 1.1 2008/10/16 07:19:21 ewv Exp $"
__version__  = "$Revision: 1.1 $"
__author__   = "ewv@fnal.gov"

import sets
import unittest
import logging

from  WMCore.SiteScreening import BlackWhiteListParser

logging.basicConfig(level=logging.DEBUG,
format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
datefmt='%m-%d %H:%M',
filename='%s.log' % __file__, filemode='w')
fakeLogger  = logging.getLogger('SiteScreening')

class FakeLogger:
    """
    Fake logger class
    """
    def __init__(self):
        pass

    def debug(self, *args):
        """
        Dummy method
        """
        pass

    def msg(self, *args):
        """
        Dummy method
        """
        pass



fakeLogger  = FakeLogger()

class BlackWhiteListParserTest(unittest.TestCase):
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
        print results
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
