#!/usr/bin/env python
# encoding: utf-8
"""
Decorators_t.py

Created by Dave Evans on 2010-07-29.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import object

import unittest
import WMCore.GroupUser.Decorators as Decs

class CrashTestDummy(object):

    def __init__(self):
        self._connected = False
        self.group = None
        self.owner = None

    connected = property(lambda x: x._connected)

    def connect(self):
        self._connected = True

    @Decs.requireConnection
    def __call__(self, x):
        return x
    @Decs.requireGroup
    def getGroup(self):
        return self.group

    @Decs.requireUser
    def getOwner(self):
        return self.owner

class Decorators_t(unittest.TestCase):

    def testA(self):
        """test requireConnection"""

        testDummy = CrashTestDummy()
        self.assertFalse(testDummy.connected)
        testDummy(99)
        self.assertTrue(testDummy.connected)

    def testB(self):
        """test requireGroup"""

        testDummy = CrashTestDummy()

        self.assertRaises(Exception, testDummy.getGroup)

        testDummy.group = "DMWM"

        self.assertEqual(testDummy.getGroup(), "DMWM")

    def testC(self):
        """test requireUser"""

        testDummy = CrashTestDummy()

        self.assertRaises(Exception, testDummy.getOwner)

        testDummy.owner = "TheRookie"

        self.assertEqual(testDummy.getOwner(), "TheRookie")


if __name__ == '__main__':
    unittest.main()
