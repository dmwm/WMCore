#!/usr/bin/env python
# encoding: utf-8
"""
WMBase.py

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""
import os
import os.path
import unittest

<<<<<<< HEAD
from WMCore.WMBase import getWMBASE, getWMTESTBASE
=======
from nose.plugins.attrib import attr

from WMCore.WMBase import getWMBASE, getTestBase
>>>>>>> remotes/dmwm/master

class WMBaseTest(unittest.TestCase):


    def testA(self):
        
        try:
            getWMBASE()
        except Exception, ex:
            self.fail("Failed to call getWMBASE")

<<<<<<< HEAD
    def testB(self):
        
        try:
            print "test base is %s" % getWMTESTBASE()
        except Exception, ex:
            self.fail("Failed to call getWMTESTBASE")

=======
        return

    @attr("integration")
    def testB_TestBase(self):
        """
        _TestBase_

        See if we can use the defaults or an environment variable
        to build a testBase

        NOTE: This has to run in WMCore/test/python/WMCore_t, and
        so is listed as integration.
        """

        test = os.path.normpath(os.path.join(os.getcwd(), '../../../test/python'))
        base = getTestBase()
        self.assertEqual(base, test)
        base = getTestBase(importFlag = False)
        self.assertEqual(base, test)
        os.environ['WMCORE_TEST_ROOT'] = '/thisdirectoryshouldneverexist'
        base = getTestBase(importFlag = False)
        self.assertEqual(base, test)
        os.environ['WMCORE_TEST_ROOT'] = '/tmp'
        base = getTestBase(importFlag = False)
        self.assertEqual(base, '/tmp')
        
        return
>>>>>>> remotes/dmwm/master

    
if __name__ == '__main__':
    unittest.main()
