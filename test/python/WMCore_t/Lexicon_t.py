#!/usr/bin/python
"""
_Lexicon_t_

General test of Lexicon

"""

__revision__ = "$Id: Lexicon_t.py,v 1.1 2009/02/03 17:47:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import unittest

from WMCore.Lexicon import *

class LexiconTest(unittest.TestCase):
    def runTest(self):
        assert sitetier('T0'), 'valid tier not validated'
        assert sitetier('T1'), 'valid tier not validated'
        assert sitetier('T2'), 'valid tier not validated'
        assert sitetier('T3'), 'valid tier not validated'
        
        self.assertRaises(AssertionError, sitetier, 'T4')
        self.assertRaises(AssertionError, sitetier, 'D0')
        
        assert cmsname('T0_CH_CERN'), 'valid CMS name not validated'
        assert cmsname('T2_UK_SGrid_Bristol'), 'valid CMS name not validated'
        self.assertRaises(AssertionError, cmsname, 'T5_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'D2_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2_UK')
            
if __name__ == "__main__":
    unittest.main() 