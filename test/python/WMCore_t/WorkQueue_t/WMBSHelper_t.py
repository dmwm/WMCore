#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WMBSHelper_t.py,v 1.1 2010/07/22 16:57:07 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import unittest

from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WorkQueueTestCase import WorkQueueTestCase

#from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WorkQueue_t.WorkQueue_t import TestReRecoFactory, rerecoArgs
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader


#TODO: This is a stub, it needs major fleshing out.

# pylint: disable=E1103


class WMBSHelperTest(WorkQueueTestCase):
    """
    _WMBSHelperTest_

    """
    def setUp(self):
        """
        setup things - db etc
        """
        WorkQueueTestCase.setUp(self)

    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)

#    def testProduction(self):
#        """Production workflow"""
#        pass

    def testReReco(self):
        """ReReco workflow"""
        # create workflow
        wmSpec = TestReRecoFactory()('ReRecoWorkload', rerecoArgs)
        inputDataset = wmSpec.taskIterator().next().inputDataset()
        dataset = wmSpec.taskIterator().next().getInputDatasetPath()
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset)}
        taskName = wmSpec.taskIterator().next().name()
        taskType = wmSpec.taskIterator().next().taskType()
        # do real tests now...
        wmbs = WMBSHelper(wmSpec, '/somewhere',
                          "whatever", taskName, taskType,
                          [], [], dataset + "#1")

        #import pdb; pdb.set_trace()
        wmbs.createSubscription()

        files = wmbs.validFiles(dbs[inputDataset.dbsurl].getFileBlock(dataset + "#1"))
        self.assertEqual(len(files), 1)

    def testReRecoRunRestriction(self):
        """ReReco workflow with Run restrictions"""
        wmSpec = TestReRecoFactory()('ReRecoWorkload', rerecoArgs)
        inputDataset = wmSpec.taskIterator().next().inputDataset()
        dataset = wmSpec.taskIterator().next().getInputDatasetPath()
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset)}
        taskName = wmSpec.taskIterator().next().name()
        taskType = wmSpec.taskIterator().next().taskType()
        #add run blacklist
        wmSpec.taskIterator().next().setInputRunBlacklist([1, 2, 3, 4])

        # Run Blacklist
        wmbs = WMBSHelper(wmSpec, '/somewhere',
                          "whatever", taskName, taskType,
                          [], [], dataset + "#2")

        wmbs.createSubscription()
        files = wmbs.validFiles(dbs[inputDataset.dbsurl].getFileBlock(dataset + "#2")[dataset + "#2"]['Files'])
        self.assertEqual(len(files), 0)

        # Run Whitelist
        wmSpec.taskIterator().next().setInputRunWhitelist([2, 3])
        wmSpec.taskIterator().next().setInputRunBlacklist([])

        wmbs = WMBSHelper(wmSpec, '/somewhere',
                          "whatever", taskName, taskType,
                          [], [], dataset + "#2")

        wmbs.createSubscription()
        files = wmbs.validFiles(dbs[inputDataset.dbsurl].getFileBlock(dataset + "#2")[dataset + "#2"]['Files'])
        self.assertEqual(len(files), 1)


# pylint: enable=E1103

if __name__ == '__main__':
    unittest.main()