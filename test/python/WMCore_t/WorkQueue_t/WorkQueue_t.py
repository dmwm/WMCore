#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueue_t.py,v 1.10 2009/09/03 13:27:17 swakef Exp $"
__version__ = "$Revision: 1.10 $"

import unittest
import pickle
import os
from WMCore.WorkQueue.WorkQueue import WorkQueue
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask

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
                                    'Size' : 100000,
                                    'Parents' : ()},
                                    {'Name' : '/fake/test/RAW#2',
                                    'NumEvents' : 1000,
                                    'NumFiles' : 10,
                                    'Size' : 300000,
                                    'Parents' : ()}
                                    ]}
        self.locations = {'/fake/test/RAW#1' : ['SiteA'],
                '/fake/test/RAW#2' : ['SiteA', 'SiteB']}

        dbsFile1 = {'Checksum': "12345",
                    'LogicalFileName': "/store/data/fake/RAW/file1",
                    'NumberOfEvents': 1000,
                    'FileSize': 102400,
                    'ParentList': []
                    }

        dbsFile2 = {'Checksum': "123456",
                    'LogicalFileName': "/store/data/fake/RECO/file2",
                    'NumberOfEvents': 1001,
                    'FileSize': 103400,
                    'ParentList': ["/store/data/fake/file2parent"]
                    }

        self.files = {'/fake/test/RAW#1' : [dbsFile1],
                      '/fake/test/RAW#2' : [dbsFile2]}

    def getFileBlocksInfo(self, dataset, onlyClosedBlocks = True):
        """Fake block info"""
        return self.blocks[dataset]

    def listFileBlockLocation(self, block):
        """Fake locations"""
        return self.locations[block]

    def listFilesInBlock(self, block):
        """Fake files"""
        return self.files[block]

    def getFileBlock(self, block):
        """Return block + locations"""
        result = { block : {
            "StorageElements" : self.listFileBlockLocation(block),
            "Files" : self.listFilesInBlock(block),
            "IsOpen" : False,
            }
                   }
        return result
# pylint: enable-msg=W0613,R0201


class WorkQueueTest(WorkQueueTestCase):
    """
    _WorkQueueTest_
    
    """
    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        WorkQueueTestCase.setUp(self)

        self.specFile = os.path.join(os.getcwd(), 'testworkflow.pickle')
        self.specName = 'testWf'
        createSpec(self.specName, self.specFile)
        self.processingSpecName = 'testProcessing'
        self.processingSpecFile = os.path.join(os.getcwd(),
                                            self.processingSpecName + ".pckl")
        createSpec(self.processingSpecName,
                   self.processingSpecFile, ['/fake/test/RAW'])
        self.__class__.queue = WorkQueue()
        mockDBS = MockDBSReader('http://example.com')
        self.__class__.queue.dbsHelpers['http://example.com'] = mockDBS


    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)

        for f in (self.specFile, self.processingSpecFile):
            try:
                os.unlink(f)
            except OSError:
                pass


    def testProduction(self):
        """
        Enqueue and get work for a production WMSpec.
        """
        specfile = self.specFile
        numBlocks = 2
        njobs = [10] * numBlocks # array of jobs per block
        total = sum(njobs)

        # Queue Work & check accepted
        for _ in range (0, numBlocks):
            self.queue.queueWork(specfile)
        # commented out for now queueWork only update the database for now
        self.assertEqual(numBlocks, len(self.queue))

        # try to get work
        work = self.queue.getWork({'SiteA' : 0})
        self.assertEqual([], work)
        work = self.queue.getWork({'SiteA' : njobs[0]})
        self.assertEqual(len(work), 1)
        # claim all work
        work = self.queue.getWork({'SiteA' : total, 'SiteB' : total})
        self.assertEqual(len(work), numBlocks - 1)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))


    def testPriority(self):
        """
        Test priority change functionality
        """
        numBlocks = 2
        njobs = [10] * numBlocks # array of jobs per block
        total = sum(njobs)

        # Queue Work & check accepted
        for _ in range (0, numBlocks):
            self.queue.queueWork(self.specFile)

        # priority change
        self.queue.setPriority(50, self.specName)
        self.assertRaises(RuntimeError, self.queue.setPriority, 50, 'blahhhhh')

        # claim all work
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), numBlocks)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))


    def testProcessing(self):
        """
        Enqueue and get work for a production WMSpec.
        """
        specfile = self.processingSpecFile
        njobs = [5, 10] # array of jobs per block
        total = sum(njobs)

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(len(njobs), len(self.queue))
        self.queue.updateLocationInfo()

        # Not quite enough resources
        work = self.queue.getWork({'SiteA' : njobs[0] - 1,
                                   'SiteB' : njobs[1] - 1})
        self.assertEqual(len(work), 0)
        # Only 1 block at SiteB
        work = self.queue.getWork({'SiteB' : total})
        self.assertEqual(len(work), 1)

        # claim remaining work
        work = self.queue.getWork({'SiteA' : total, 'SiteB' : total})
        self.assertEqual(len(work), 1)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))


    def testBlackList(self):

        specfile = self.processingSpecFile
        njobs = [5, 10] # array of jobs per block
        numBlocks = len(njobs)
        total = sum(njobs)

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numBlocks, len(self.queue))

        #In blacklist
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0) # Fail here till blacklist works

        fakeDBS = self.queue.dbsHelpers['http://example.com']
        fakeDBS.locations['/fake/test/RAW#1'] = ['SiteA', 'SiteB']
        self.queue.updateLocationInfo()

        # SiteA still blacklisted for all blocks - but not supported
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0)
        # SiteB can run all blocks now
        work = self.queue.getWork({'SiteB' : total})
        self.assertEqual(len(work), 2)

        #TODO: Add whitelist test here


    def runTest(self):
        """run all tests"""
        self.testProduction()
        self.testProcessing()
        self.testPriority()
        self.testBlackList()


if __name__ == "__main__":
    unittest.main()
