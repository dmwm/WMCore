#!/usr/bin/env python
# encoding: utf-8
"""
Interface_t.py

Created by Dave Evans on 2010-07-29.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os

from WMCore.GroupUser.Interface import Interface


class Interface_t(unittest.TestCase):

    def setUp(self):
         self.url = os.getenv("COUCHURL", "http://127.0.0.1:5984")
         self.database = "groupuser"
    
    def testA(self):
        guInt = Interface(self.url, self.database)
    
if __name__ == '__main__':
    unittest.main()
