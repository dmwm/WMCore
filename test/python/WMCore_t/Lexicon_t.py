#!/usr/bin/python
"""
_Lexicon_t_

General test of Lexicon

"""

__revision__ = "$Id: Lexicon_t.py,v 1.2 2009/07/27 16:07:43 metson Exp $"
__version__ = "$Revision: 1.2 $"

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
        assert cmsname('T2'), 'partial CMS name not validated'
        assert cmsname('T2_'), 'partial CMS name not validated'
        assert cmsname('T2_UK'), 'partial CMS name not validated'
        assert cmsname('T2_UK_'), 'partial CMS name not validated'
        assert cmsname('T2_UK_SGrid'), 'partial CMS name not validated'
        assert cmsname('T2_UK_SGrid_'), 'partial CMS name not validated'
        
    def testBadCMSName(self):        
        # Check that invalid names raise an exception
        self.assertRaises(AssertionError, cmsname, 'T5_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'D2_UK_SGrid_Bristol')
        #self.assertRaises(AssertionError, cmsname, 'T2_UK')
            
if __name__ == "__main__":
    unittest.main() 