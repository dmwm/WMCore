#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__DashboardAPI_t__
Unit tests for the dashboard reporter
Created on Tue Feb 23 13:30:04 2016

@author: jbalcas
"""
from __future__ import print_function, division
from builtins import range
import unittest

import os
import time

from WMCore.WMBase import getTestBase
from WMCore.Services.Dashboard.Logger import Logger
from WMCore.Services.Dashboard.DashboardAPI import (DashboardAPI, parseAd,
                                                    reportFailureToDashboard, logger)


class DashboardAPITest(unittest.TestCase):
    """
    _DashboardAPITest_

    Unit tests for the dashboard API and apmon.
    """
    def testApmonInstance(self):
        """
        _testApmonInstance_

        Just test initialization of apmon Instance
        """
        with DashboardAPI() as dashboard:
            self.assertTrue(dashboard.apmon.initializedOK())

    def testApmonSend(self):
        """
        _testApmonSend_

        We cannot really test any results, but let's just send some data
        """
        package = {'jobId': 'abcd-1234_0', 'MessageType': 'JobStatus',
                   'taskId': 'wmagent_alan', 'StatusValue': 'submitted'}

        with DashboardAPI() as dashboard:
            dashboard.apMonSend(package)

    def testLogger(self):
        """
        _testLogger_

        Just test logger and if it writes correct log line
        """
        # Write a line and read the last line, it should be equal
        testMsg = "This is a test of looger write. Timespan %s\n" % time.time()
        logger(testMsg)
        lines = []
        with open("report.log") as fd:
            lines = fd.readlines()
        self.assertEqual(testMsg, lines[-1])
        # Write a line without \n ending and test it
        testMsg = "This is a test of looger write. Timespan %s" % time.time()
        logger(testMsg)
        lines = []
        with open("report.log") as fd:
            lines = fd.readlines()
        self.assertNotEqual(testMsg, lines[-1])
        self.assertEqual(testMsg, lines[-1].strip())

    def testLoggerLevels(self):
        """
        _testLoggerLevels_

        Just test logger levels and if correct message is printed
        """
        logging = Logger()
        for level in range(len(logging.LEVELS)):
            testString = "Test logging level"
            logging.setLogLevel(level)
            logging.log(level, testString)

    def testreportFailureToDashboard(self):
        """
        _testreportFailureToDashboard_

        test report Failure to dashboard with/without job_ad file
        """
        self.assertEqual(0, reportFailureToDashboard(0, None))
        # parseAd is called by reportFailureToDashboard, but if we would
        # call it from reportFailureToDashboard error will be suppressed
        # and it will return same exit code
        # So lets call it directly
        if '_CONDOR_JOB_AD' in os.environ:
            del os.environ["_CONDOR_JOB_AD"]
        self.assertRaises(KeyError, parseAd)
        self.assertEqual(0, reportFailureToDashboard(0))
        os.environ["_CONDOR_JOB_AD"] = "testFile"
        # if this environment variable is set, it will check this file
        # if file is not present, it will raise IOError
        self.assertRaises(IOError, parseAd)
        # Test real job_ad file and check if it returns dict.
        os.environ["_CONDOR_JOB_AD"] = os.path.join(getTestBase(), '..', 'data', 'WMCore', 'Services', 'Dashboard', 'job_ad_file')
        out = parseAd()
        self.assertTrue(isinstance(out, dict))
        # Also test it sending directly parsed job ad
        self.assertEqual(0, reportFailureToDashboard(0, out))

    def testDashboardAPI(self):
        """
        _testDashboardAPI_

        Test dashboard API which can be used through python to publish values
        """
        os.environ["_CONDOR_JOB_AD"] = os.path.join(getTestBase(), '..', 'data', 'WMCore', 'Services', 'Dashboard', 'job_ad_file')
        out = parseAd()
        with DashboardAPI() as dashboardAPI:
            self.assertTrue(isinstance(out, dict))
            self.assertEqual(None, dashboardAPI.publish())
            self.assertEqual(None, dashboardAPI.sendValues(out))
            self.assertEqual(None, dashboardAPI.sendValues(out, out['CRAB_Id']))
            self.assertEqual(None, dashboardAPI.sendValues(out, out['CRAB_Id'], out['CRAB_ReqName']))

if __name__ == '__main__':
    unittest.main()
