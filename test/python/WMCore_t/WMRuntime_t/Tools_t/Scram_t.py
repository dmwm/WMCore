#!/usr/bin/env python
# encoding: utf-8
"""
Scram_t.py

Created by Dave Evans on 2012-01-27.
Copyright (c) 2012 evansde77. All rights reserved.
"""

import unittest
import os
import tempfile
from WMQuality.TestInit import TestInit
from Utils.TemporaryEnvironment import tmpEnv
from WMCore.WMRuntime.Tools.Scram import Scram, OS_TO_ARCH, ARCH_TO_OS, getSingleScramArch


class Scram_t(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()
        self.oldCwd = os.getcwd()

    def tearDown(self):
        self.testInit.delWorkDir()

    def testA(self):
        """
        instantiate a Scram instance in test mode.
        """
        try:
            Scram(
                initialise="/bin/date",
                architecture="slc5_amd64_gcc454",
                version="CMSSW_X_Y_Z",
                test=True
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
            Scram(
                initialise="/bin/date",
                architecture="slc5_amd64_gcc454",
                version="CMSSW_X_Y_Z"
            )
        except Exception as ex:
            msg = "Failed to instantiate Scram:\n %s " % str(ex)
            self.fail(msg)

    def testC(self):
        """
        test all method calls in test mode

        """
        s = Scram(
            initialise="/bin/date",
            architecture="slc5_amd64_gcc454",
            version="CMSSW_X_Y_Z",
            directory=self.testDir,
            test=True

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

    def testArchMap(self):
        self.assertItemsEqual(OS_TO_ARCH['rhel6'], ['slc5', 'slc6'])
        self.assertItemsEqual(OS_TO_ARCH['rhel7'], ['slc7'])
        self.assertItemsEqual(ARCH_TO_OS['slc6'], ['rhel6'])
        self.assertItemsEqual(ARCH_TO_OS['slc7'], ['rhel7'])

    def testScramArchParsing(self):
        """
        Test the various modes of parsing for the scram arch
        """
        try:
            os.chdir(self.testDir)
            with tempfile.NamedTemporaryFile() as tf:
                tf.write('GLIDEIN_REQUIRED_OS = "rhel6" \n')
                tf.write('Memory = 2048\n')
                tf.flush()
                with tmpEnv(_CONDOR_MACHINE_AD=tf.name):
                    self.assertEqual(getSingleScramArch('slc6_blah_blah'), 'slc6_blah_blah')
                    self.assertEqual(getSingleScramArch('slc5_blah_blah'), 'slc5_blah_blah')
                    self.assertEqual(getSingleScramArch(['slc6_blah_blah', 'slc7_blah_blah']),
                                      'slc6_blah_blah')
                    self.assertEqual(getSingleScramArch(['slc6_blah_blah', 'slc5_blah_blah']),
                                      'slc6_blah_blah')
                    self.assertEqual(getSingleScramArch(['slc7_blah_blah', 'slc8_blah_blah']), None)
            with tempfile.NamedTemporaryFile() as tf:
                tf.write('GLIDEIN_REQUIRED_OS = "rhel7" \n')
                tf.write('Memory = 2048\n')
                tf.flush()
                with tmpEnv(_CONDOR_MACHINE_AD=tf.name):
                    self.assertEqual(getSingleScramArch('slc6_blah_blah'), 'slc6_blah_blah')
                    self.assertEqual(getSingleScramArch('slc7_blah_blah'), 'slc7_blah_blah')
                    self.assertEqual(getSingleScramArch(['slc6_blah_blah', 'slc7_blah_blah']),
                                      'slc7_blah_blah')
                    self.assertEqual(getSingleScramArch(['slc6_blah_blah', 'slc5_blah_blah']), None)
                    self.assertEqual(getSingleScramArch(['slc7_blah_blah', 'slc8_blah_blah']),
                                      'slc7_blah_blah')
        except Exception:
            raise
        finally:
            os.chdir(self.oldCwd)
        return


if __name__ == '__main__':
    unittest.main()
