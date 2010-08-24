#!/usr/bin/env python
"""
Test case for wmbs
"""


import sets
import unittest

#import common
from  WMCore.SiteScreening import BlackWhiteListParser

class FakeLogger:

    def debug(*args):
        pass

    def msg(*args):
        pass

class TestBlackWhiteList(unittest.TestCase):

    def setUp(self):
        self.separser = BlackWhiteListParser.SEBlackWhiteListParser(cfgParams, fakeLogger)
        self.ceparser = BlackWhiteListParser.CEBlackWhiteListParser(cfgParams, fakeLogger)

    def test_se_black_list(self):
        blacklist = ['ccsrm.in2p3.fr', 'cmssrm.fnal.gov']
        other = ['t2-srm-02.lnl.infn.it', 'se-dcache.hepgrid.uerj.br']
        results = self.separser.checkBlackList(other + blacklist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(other))

    def test_se_white_list(self):
        whitelist = ['srm.ihepa.ufl.edu', 'heplnx204.pp.rl.ac.uk',
            'cluster142.knu.ac.kr']
        other = ['f-dpm001.grid.sinica.edu.tw', 'cmsrm-se01.roma1.infn.it']
        results = self.separser.checkWhiteList(other + whitelist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(whitelist))

    def test_ce_black_list(self):
        blacklist = ['lcg02.ciemat.es', 'lcgce01.phy.bris.ac.uk',
            'lcgce02.phy.bris.ac.uk', 'cmsosgce4.fnal.gov']
        other = ['osgce.hepgrid.uerj.br', 'egeece01.ifca.es',
            'grid006.lca.uc.pt']
        results = self.ceparser.checkBlackList(other + blacklist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(other))

    def test_ce_white_list(self):
        whitelist = ['vampire.accre.vanderbilt.edu',
            'ic-kit-lcgce.rz.uni-karlsruhe.de']
        other = ['gridce2.pi.infn.it', 'lcg02.ciemat.es']
        results = self.ceparser.checkWhiteList(other + whitelist)
        results = sets.Set(results)
        self.failUnless(results == sets.Set(whitelist))


cfgParams = {
  'EDG.se_black_list': 'ccsrm.in2p3.fr, T1_*',
  'EDG.se_white_list': 'T2_US, T2_UK, T2_KR_KNU',
  'EDG.ce_black_list': 'lcg02.ciemat.es, bris.ac, *.fnal.gov',
  'EDG.ce_white_list': 'T3',
}

fakeLogger = FakeLogger()



if __name__ == '__main__':
#    common.logger = FakeLogger()
    unittest.main()

