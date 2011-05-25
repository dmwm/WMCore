#!/usr/bin/env python
# encoding: utf-8
"""
WMInit_t.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
from WMCore.WMInit import getWMBASE

class WMInit_t(unittest.TestCase):

    def testA(self):
        
        try:
            getWMBASE()
        except:
            self.fail("Error calling WMInit.getWMBASE")
    
    
if __name__ == '__main__':
    unittest.main()