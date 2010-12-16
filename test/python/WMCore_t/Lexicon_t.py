#!/usr/bin/python
"""
_Lexicon_t_

General test of Lexicon

"""




import logging
import unittest

from WMCore.Lexicon import *

class LexiconTest(unittest.TestCase):
    def testGoodSiteTier(self):
        # Check that valid tiers work
        assert sitetier('T0'), 'valid tier not validated'
        assert sitetier('T1'), 'valid tier not validated'
        assert sitetier('T2'), 'valid tier not validated'
        assert sitetier('T3'), 'valid tier not validated'
        
    def testBadSiteTier(self):    
        # Check that invalid tiers raise an exception
        self.assertRaises(AssertionError, sitetier, 'T4')
        self.assertRaises(AssertionError, sitetier, 'D0')
    
    def testGoodCMSName(self):    
        # Check that full names work
        assert cmsname('T0_CH_CERN'), 'valid CMS name not validated'
        assert cmsname('T2_UK_SGrid_Bristol'), 'valid CMS name not validated'
        
    def testPartialCMSName(self):    
        # Check that partial names work
        for i in ['T%', 'T2','T2_', 'T2_UK', 'T2_UK_', 'T2_UK_SGrid', 'T2_UK_SGrid_']:
            assert cmsname(i), 'partial CMS name (%s) not validated' % i
        
    def testBadCMSName(self):        
        # Check that invalid names raise an exception
        self.assertRaises(AssertionError, cmsname, 'T5_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2-UK-SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2_UK_SGrid_Bris-tol')
        self.assertRaises(AssertionError, cmsname, 'D2_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2asjkjhadshjkdashjkasdkjhdas')
        #self.assertRaises(AssertionError, cmsname, 'T2_UK')

    def testGoodIdentifier(self):
        assert identifier('__wil.1.am__')

    def testBadIdentifier(self):
        self.assertRaises(AssertionError, identifier, 'ke$ha')

    def testGoodDataset(self):
        assert dataset('/a/b/c')
        assert dataset('/m000n/RIII-ver/wider_than_1.0_miles')

    def testBadDataset(self):
        for notok in ['/Sugar/Sugar', '/Oh/honey/honey!', '/You/are/my/candy/GIIIRRL']:
           self.assertRaises(AssertionError, dataset, notok)

    def testVersion(self):
        for ok in ['CMSSW_3_8_0_pre1', 'CMSSW_1_2_0', 'CMSSW_4_0_0_patch11']:
            assert cmsswversion(ok)

    def testBadVersion(self):
        for notok in ['ORCA_3_8_0', 'CMSSW_3_5']:
            self.assertRaises(AssertionError, cmsswversion, notok)

    def testGoodCouchUrl(self):
        for ok in ['http://vittoria@antimatter.cern.ch:5984', 'http://fbi.fnal.gov:5984', 
                   'http://fmulder:trustno1@fbi.fnal.gov:5984']:
            assert couchurl(ok)

    def testBadCouchUrl(self):
        for notok in ['agent86@control.fnal.gov:5984', 'http://assange:password@interpol.cern.ch:5985']:
            self.assertRaises(AssertionError, couchurl, notok)
            
if __name__ == "__main__":
    unittest.main() 
