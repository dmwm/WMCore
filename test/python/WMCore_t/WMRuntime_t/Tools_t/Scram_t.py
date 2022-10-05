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
from Utils.PythonVersion import PY3
from WMCore.WMRuntime.Tools.Scram import (Scram, OS_TO_ARCH, ARCH_TO_OS, getSingleScramArch,
                                          isCMSSWSupported, isEnforceGUIDInFileNameSupported)


class Scram_t(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()
        self.oldCwd = os.getcwd()
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

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
        self.assertItemsEqual(OS_TO_ARCH['rhel8'], ['el8', 'cc8', 'cs8', 'alma8'])
        self.assertItemsEqual(ARCH_TO_OS['slc6'], ['rhel6'])
        self.assertEqual(len(ARCH_TO_OS), 7)
        self.assertItemsEqual(ARCH_TO_OS['slc7'], ['rhel7'])
        self.assertItemsEqual(ARCH_TO_OS['slc7'], ['rhel7'])

    def testScramArchParsing(self):
        """
        Test the various modes of parsing for the scram arch
        """
        try:
            os.chdir(self.testDir)
            with tempfile.NamedTemporaryFile() as tf:
                tf.write(b'GLIDEIN_REQUIRED_OS = "rhel6" \n')
                tf.write(b'Memory = 2048\n')
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
                tf.write(b'GLIDEIN_REQUIRED_OS = "rhel7" \n')
                tf.write(b'Memory = 2048\n')
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

    def testCMSSWSupported(self):
        """
        Test the functionality of isCMSSWSupported function
        """
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3', ''))
        self.assertFalse(isCMSSWSupported(None, 'a'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3', 'CMSSW_2_2_3'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3', 'CMSSW_1_3_3'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3', 'CMSSW_1_2_4'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3_pre1', 'CMSSW_1_2_3'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3', 'CMSSW_1_2_3_pre1'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3_pre1', 'CMSSW_1_2_3_pre2'))
        self.assertFalse(isCMSSWSupported('CMSSW_1_2_3_pre2', 'CMSSW_1_2_3_pre1'))
        self.assertFalse(isCMSSWSupported('CMSSW_7_1_25_patch2', 'CMSSW_7_6_0'))
        self.assertFalse(isCMSSWSupported('CMSSW_7_3_2', 'CMSSW_10_4_0'))

        self.assertTrue(isCMSSWSupported('CMSSW_1_2_3_pre1', 'CMSSW_1_2_3_pre1'))
        self.assertTrue(isCMSSWSupported('CMSSW_1_2_3', 'CMSSW_1_2_3'))
        self.assertTrue(isCMSSWSupported('CMSSW_2_2_3', 'CMSSW_1_2_3'))
        self.assertTrue(isCMSSWSupported('CMSSW_1_3_3', 'CMSSW_1_2_3'))
        self.assertTrue(isCMSSWSupported('CMSSW_1_2_4', 'CMSSW_1_2_3'))

    def testisEnforceGUIDInFileNameSupported(self):
        """
        Test functionality of the `isEnforceGUIDInFileNameSupported` function
        """
        ### invalid input
        self.assertFalse(isEnforceGUIDInFileNameSupported(None))
        self.assertFalse(isEnforceGUIDInFileNameSupported(''))

        ### forever supported
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_11_0_0'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_11_0_2'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_11_1_0_pre1'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_11_1_0_patch1'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_11_1_1'))

        ### specific releases
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_10_2_20_UL'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_9_4_16_UL'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_8_0_34_UL'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_7_1_45_patch3'))

        ### minor supported releases
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_10_6_8'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_10_6_9'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_10_6_8_patch1'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_10_6_9_patch1'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_10_2_20'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_9_4_16'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_9_3_17'))
        self.assertTrue(isEnforceGUIDInFileNameSupported('CMSSW_8_0_34'))

        ### releases not supported
        self.assertFalse(isEnforceGUIDInFileNameSupported('CMSSW_10_6_7'))
        self.assertFalse(isEnforceGUIDInFileNameSupported('CMSSW_10_7_0'))
        self.assertFalse(isEnforceGUIDInFileNameSupported('CMSSW_10_2_19'))
        self.assertFalse(isEnforceGUIDInFileNameSupported('CMSSW_10_3_10'))
        self.assertFalse(isEnforceGUIDInFileNameSupported('CMSSW_5_3_10'))


if __name__ == '__main__':
    unittest.main()
