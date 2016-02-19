#!/usr/bin/env python
# encoding: utf-8
"""
Locale_t.py

Created by Dave Evans on 2011-05-23.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
from WMCore.Locale import Locale
from WMCore.Configuration import Configuration

class Locale_t(unittest.TestCase):

    def testA(self):
        """check sections/fields exist in Locale template"""
        sections = ['reqmgr', 'gwq', 'lwq', 'agent', 'crabserver', 'couch', 'mysql']
        for s in sections:
            self.assertTrue(hasattr(Locale.locale, s))

    def testB(self):
        """check set/get"""

        Locale.locale.mysql.url = "mysql://whatever:3306"
        Locale.locale.couch.port = "1999"

        self.assertEqual(Locale.locale.mysql.url, "mysql://whatever:3306")
        self.assertEqual(Locale.locale.couch.port, "1999")

    def testC(self):
        """test addition of locale to configuration instance"""

        conf = Configuration()

        conf + Locale
        self.assertTrue(hasattr(conf, 'locale'))



if __name__ == '__main__':
    unittest.main()
