#!/usr/bin/env python
# encoding: utf-8
"""
Scram_t.py

Created by Dave Evans on 2012-01-27.
Copyright (c) 2012 evansde77. All rights reserved.
"""

import unittest
import os
from WMQuality.TestInit import TestInit
from WMCore.WMRuntime.Tools.Scram import Scram

class Scram_t(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()

    def tearDown(self):
        self.testInit.delWorkDir()

    def testA(self):
        """
        instantiate a Scram instance in test mode.
        """
        try:
            s = Scram(
                initialise = "/bin/date",
                architecture = "slc5_amd64_gcc454",
                version = "CMSSW_X_Y_Z",
                test = True
            )
        except Exception as ex:
            msg = "Failed to instantiate Scram in test mode:\n %s " % str(ex)
            self.fail(msg)


    def testB(self):
        """
        instantiante a Scram instance in non-test mode
        limited what we can test here since we dont have scram etc in unittest env
        """
        try:
            s = Scram(
                initialise = "/bin/date",
                architecture = "slc5_amd64_gcc454",
                version = "CMSSW_X_Y_Z"
            )
        except Exception as ex:
            msg = "Failed to instantiate Scram:\n %s " % str(ex)
            self.fail(msg)



    def testC(self):
        """
        test all method calls in test mode

        """
        s = Scram(
            initialise = "/bin/date",
            architecture = "slc5_amd64_gcc454",
            version = "CMSSW_X_Y_Z",
            directory = self.testDir,
            test = True

        )

        try:
            status = s.project()
        except Exception as ex:
            msg = "Error running Scram.project:\n %s" % str(ex)
            self.fail(msg)

        self.assertEqual(status, 0)
        self.assertTrue(os.path.exists(s.projectArea))
        self.assertTrue("project" in s.lastExecuted)
        self.assertTrue("CMSSW_X_Y_Z" in s.lastExecuted)

        try:
            status = s.runtime()
        except Exception as ex:
            msg = "Error running Scram.runtime:\n %s" % str(ex)
            self.fail(msg)

        self.assertEqual(status, 0)
        self.assertTrue("ru -sh" in s.lastExecuted)
        self.assertTrue("TEST_MODE" in s.runtimeEnv)

        comm = "echo \"Hello World\""
        try:
            status = s(comm)
        except Exception as ex:
            msg = "Failed to call Scram object:\n %s" % str(ex)

        self.assertEqual(status, 0)
        self.assertEqual(s.lastExecuted, comm)


if __name__ == '__main__':
    unittest.main()
