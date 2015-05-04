#!/usr/bin/env python
# encoding: utf-8
"""
Service_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest


from WMCore.ACDC.Service import Service

class Service_t(unittest.TestCase):

    def testA(self):
        """test base class that pretty much does bugger all..."""

        try:
            s = Service()
        except Exception as ex:
            msg = "Failed to instantiate Service: %s" % str(ex)
            self.fail(msg)


if __name__ == '__main__':
    unittest.main()
