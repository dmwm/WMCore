#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueueElement_t.py,v 1.2 2009/10/13 22:42:56 meloam Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import pickle
import os
from WMCore.WorkQueue.WorkQueue import WorkQueue, _WQElement
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask
from WMCore.WorkQueue.WorkSpecParser import WorkSpecParser

from WorkQueueTestCase import WorkQueueTestCase

def createSpec(name, path, dataset = None):
    """
    create a wmspec object and save to disk
    """
    wmspec = newWorkload(name)
    task = makeWMTask('task1')
    if dataset:
        task.data.parameters.inputDatasets = dataset
        task.data.parameters.splitType = 'File'
        task.data.parameters.splitSize = 1
        task.data.constraints.sites.blacklist = ['SiteA']
        wmspec.data.dbs = 'http://example.com'
    else:
        task.data.parameters.splitType = 'Event'
        task.data.parameters.splitSize = 100
        task.data.parameters.totalEvents = 1000
    wmspec.addTask(task)
    out = open(path, 'wb')
    pickle.dump(wmspec, out)
    out.close()


# //  mock dbs info - ignore a lot of arguments
#//     - ignore some params in dbs spec - silence pylint warnings
# pylint: disable-msg=W0613,R0201
class MockDBSReader:
    """
    Mock up dbs access
    """
    def __init__(self, url):
        self.blocks = {'/fake/test/RAW': [{'Name' : '/fake/test/RAW#1',
                                    'NumEvents' : 500,
                                    'NumFiles' : 5,
                                    'Parents' : ()},
                                    {'Name' : '/fake/test/RAW#2',
                                    'NumEvents' : 1000,
                                    'NumFiles' : 10,
                                    'Parents' : ()}
                                    ]}
        self.locations = {'/fake/test/RAW#1' : ['SiteA'],
                '/fake/test/RAW#2' : ['SiteA', 'SiteB']}
    
    def getFileBlocksInfo(self, dataset, onlyClosedBlocks=True):
        """Fake block info"""
        return self.blocks[dataset]
    
    def listFileBlockLocation(self, block):
        """Fake locations"""
        return self.locations[block]
# pylint: enable-msg=W0613,R0201
        
        
class WorkQueueElementTest(WorkQueueTestCase):
    """
    _WorkQueueElementTest_
    
    """
    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        WorkQueueTestCase.setUp(self)
        
        self.specFile = os.path.join(os.getcwd(), 'testworkflow.pickle')
        self.specName = 'testWf'
        createSpec(self.specName, self.specFile)
        self.specHelper = WorkSpecParser(self.specFile)


    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)
        
        try:
            os.unlink(self.specFile)
        except OSError:
            pass


    def testOrdering(self):
        """
        Test priority sorting
        """
        ele1 = _WQElement(self.specHelper, 1)
        ele2 = _WQElement(self.specHelper, 1)
        ele2.priority = 2
        self.assertTrue(ele2 < ele1)
        ele1.priority = 3
        self.assertTrue(ele1 < ele2)
        # ensure old jobs rise in priority - very basic check
        ele2.insertTime = 0
        self.assertTrue(ele2 < ele1)


    def testMatch(self):
        """
        Test elements match correctly
        """
        condition = {'SiteA' : 100}
        ele = _WQElement(self.specHelper, 50)
        matched, _ = ele.match({'SiteA' : 49})
        self.assertFalse(matched)
        matched, condition = ele.match(condition)
        self.assertTrue(matched)
        self.assertEqual(condition, {'SiteA' : 50})
        matched, condition = ele.match(condition)
        self.assertTrue(matched)
        self.assertEqual(condition, {})
        matched, condition = ele.match(condition)
        self.assertFalse(matched)
        ele.setStatus("Acquired")
        self.assertEqual("Acquired", ele.status)


if __name__ == "__main__":
    unittest.main()