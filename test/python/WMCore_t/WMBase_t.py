#!/usr/bin/env python
# encoding: utf-8
"""
WMBase.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest

from WMCore.WMBase import getWMBASE

class WMBaseTest(unittest.TestCase):


    def testA(self):
        
        try:
            getWMBASE()
        except Exception, ex:
            self.fail("Failed to call getWMBASE")

    
if __name__ == '__main__':
    unittest.main()