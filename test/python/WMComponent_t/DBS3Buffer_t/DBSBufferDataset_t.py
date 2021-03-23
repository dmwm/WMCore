"""
_DBSBufferDataset_t

Test module for the DBSBufferDataset object

Created on May 2, 2013

@author: dballest
"""
import os
import unittest
import threading

from WMComponent.DBS3Buffer.DBSBufferDataset import DBSBufferDataset

from WMCore.WMBase import getTestBase
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

from WMQuality.TestInit import TestInit

class DBSBufferDatasetTest(unittest.TestCase):
    """
    _DBSBufferDatasetTest_

    Collection of tests for the DBSBuffer object
    """
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMComponent.DBS3Buffer"],
                                useDefault = False)

    def tearDown(self):
        self.testInit.clearDatabase()

    def testBasic(self):
        """
        _testBasic_

        Test the basic functions of the DBSBufferDataset,
        create, load, exists and also the ability
        to add subscriptions.
        """
        originalDataset = DBSBufferDataset(path = '/bogus/bogus/go')
        originalDataset.create()
        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT id FROM dbsbuffer_dataset")[0].fetchall()
        self.assertEqual(originalDataset.exists(), result[0][0])
        duplicateDataset = DBSBufferDataset(path = '/bogus/bogus/go')
        duplicateDataset.create()
        self.assertEqual(originalDataset.exists(), duplicateDataset.exists())
        result = myThread.dbi.processData("SELECT COUNT(id) FROM dbsbuffer_dataset")[0].fetchall()
        self.assertEqual(result[0][0], 1)
        loadedDataset = DBSBufferDataset(path = '/bogus/bogus/go')
        loadedDataset.load()
        self.assertEqual(loadedDataset.exists(), originalDataset.exists())
        secondDataset = DBSBufferDataset(path = '/BogusPrimary/Run2012Z-PromptReco-v1/RECO')
        secondDataset.create()
        workload = WMWorkloadHelper()
        workload.load(os.path.join(getTestBase(), 'WMComponent_t/DBS3Buffer_t/specs/TestWorkload.pkl'))
        secondDataset.addSubscription(workload.getSubscriptionInformation()['/BogusPrimary/Run2012Z-PromptReco-v1/RECO'])
        secondDataset.addSubscription(workload.getSubscriptionInformation()['/BogusPrimary/Run2012Z-PromptReco-v1/RECO'])
        self.assertEqual(len(secondDataset['subscriptions']), 3)
        result = myThread.dbi.processData("SELECT COUNT(id) FROM dbsbuffer_dataset_subscription")[0].fetchall()
        self.assertEqual(result[0][0], 3)
        return

if __name__ == "__main__":
    unittest.main()
