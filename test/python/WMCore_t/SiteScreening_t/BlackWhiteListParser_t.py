#!/usr/bin/env python
"""
Test case for SiteScreening
"""

import unittest
import logging

from  WMCore.SiteScreening import BlackWhiteListParser

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__, filemode='w')
fakeLogger = logging.getLogger('SiteScreening')


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

    def info(self, *args):
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
            BlackWhiteListParser.SEBlackWhiteListParser(
                whiteList=seWhiteList,
                blackList=seBlackList,
                logger=fakeLogger
            )

        self.ceparser = \
            BlackWhiteListParser.CEBlackWhiteListParser(
                whiteList=ceWhiteList,
                blackList=ceBlackList,
                logger=fakeLogger
            )



    def testSEBlackList(self):
        """
        Tests black list parsing for Storage Elements
        """
        blacklist = ['ccsrm.in2p3.fr', 'cmssrm.fnal.gov']
        other = ['t2-srm-02.lnl.infn.it', 'se-dcache.hepgrid.uerj.br']
        results = self.separser.checkBlackList(other + blacklist)
        results = set(results)
        self.assertTrue(results == set(other))

    def testSEWhiteList(self):
        """
        Tests white list parsing for Storage Elements
        """
        whitelist = ['srm.ihepa.ufl.edu', 'heplnx204.pp.rl.ac.uk',
            'cluster142.knu.ac.kr']
        other = ['f-dpm001.grid.sinica.edu.tw', 'cmsrm-se01.roma1.infn.it']
        results = self.separser.checkWhiteList(other + whitelist)
        results = set(results)
        self.assertTrue(results == set(whitelist))

    def testCEBlackList(self):
        """
        Tests black list parsing for Compute Elements
        """
        blacklist = ['lcg02.ciemat.es', 'lcgce01.phy.bris.ac.uk',
            'lcgce02.phy.bris.ac.uk', 'cmsosgce4.fnal.gov']
        other = ['osgce.hepgrid.uerj.br', 'egeece01.ifca.es',
            'grid006.lca.uc.pt']
        results = self.ceparser.checkBlackList(other + blacklist)
        results = set(results)
        self.assertTrue(results == set(other))

    def testCEWhiteList(self):
        """
        Tests white list parsing for Compute Elements
        """
        whitelist = ['vampire.accre.vanderbilt.edu',
                     'ic-kit-lcgce.rz.uni-karlsruhe.de']
        other = ['gridce2.pi.infn.it', 'lcg02.ciemat.es']
        results = self.ceparser.checkWhiteList(other + whitelist)
        results = set(results)
        self.assertTrue(results == set(whitelist))


seBlackList = 'ccsrm.in2p3.fr, T1_*'
seWhiteList = 'srm.ihepa.ufl.edu, heplnx204.pp.rl.ac.uk, cluster142.knu.ac.kr'
ceBlackList = 'lcg02.ciemat.es, bris.ac, *.fnal.gov'
ceWhiteList = 'vampire.accre.vanderbilt.edu, ic-kit-lcgce.rz.uni-karlsruhe.de'



if __name__ == '__main__':
    unittest.main()
