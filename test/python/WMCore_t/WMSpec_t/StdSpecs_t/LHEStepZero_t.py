#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_LHEStepZero_t_
Created on Mon May  7 23:09:16 2012

LHE Step0 workflow unit test

@author: dballest
"""

import unittest
import os
import threading

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.LHEStepZero import getTestArguments, lheStepZeroWorkload

from WMCore_t.WMSpec_t.StdSpecs_t.MonteCarlo_t import MonteCarloTest

class LHEStepZeroTest(MonteCarloTest):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.

        """
        MonteCarloTest.setUp(self)

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.

        """
        MonteCarloTest.tearDown(self)

    def testLHEStepZero(self):
        """
        _testLHEStepZero_
        Make sure that the workload can be created
        and complies with the common MonteCarlo test
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "rereco_t"
        defaultArguments["ConfigCacheID"] = self.injectMonteCarloConfig()

        testWorkload = lheStepZeroWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")

        testWMBSHelper = WMBSHelper(testWorkload, "Production", "SomeBlock", cachepath = self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._commonMonteCarloTest()

if __name__ == '__main__':
    unittest.main()
