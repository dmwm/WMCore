#!/usr/bin/env python
# encoding: utf-8
"""
Group_t.py

Created by Dave Evans on 2010-07-29.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os

from WMCore.GroupUser.Group import Group

class Group_t(unittest.TestCase):

    def setUp(self):
        self.database = "groupuser"
        self.url = os.getenv("COUCHURL", "http://127.0.0.1:5984")


    def testA(self):
        """instantiate and jsonise"""

        g1 = Group(name = "DMWM", administrators = ['evansde77', 'drsm79'])

        g1.setCouch(self.url, self.database)
        g1.create()

        g2 = Group(name = "DMWM")
        g2.setCouch(self.url, self.database)
        g2.get()

        g1.drop()

if __name__ == '__main__':
    unittest.main()
