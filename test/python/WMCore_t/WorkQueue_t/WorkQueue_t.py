#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueue_t.py,v 1.25 2010/01/28 13:53:21 swakef Exp $"
__version__ = "$Revision: 1.25 $"

import unittest
import os
import shutil
from copy import deepcopy, copy

from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask

from WorkQueueTestCase import WorkQueueTestCase

from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload
from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload import workload as MultiTaskProductionWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workingDir
shutil.rmtree(workingDir, ignore_errors = True)
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
from WMCore_t.WorkQueue_t.MockPhedexService import MockPhedexService

# NOTE: All queues point to the same database backend
# Thus total element counts etc count elements in all queues


class WorkQueueTest(WorkQueueTestCase):
    """
    _WorkQueueTest_
    
    """
    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        WorkQueueTestCase.setUp(self)

        # Basic production Spec
        self.spec = BasicProductionWorkload
        self.spec.setSpecUrl(os.path.join(self.workDir, 'testworkflow.spec'))
        self.spec.save(self.spec.specUrl())

        # Sample Tier1 ReReco spec
        self.processingSpec = Tier1ReRecoWorkload
        self.processingSpec.setSpecUrl(os.path.join(self.workDir,
                                                    'testProcessing.spec'))
        self.processingSpec.save(self.processingSpec.specUrl())

        # ReReco spec with blacklist
        self.blacklistSpec = deepcopy(self.processingSpec)
        self.blacklistSpec.setSpecUrl(os.path.join(self.workDir,
                                                    'testBlacklist.spec'))
        self.blacklistSpec.taskIterator().next().data.constraints.sites.blacklist = ['SiteA']
        self.blacklistSpec.data._internal_name = 'blacklistSpec'
        self.blacklistSpec.save(self.blacklistSpec.specUrl())

        # ReReco spec with whitelist
        self.whitelistSpec = deepcopy(self.processingSpec)
        self.whitelistSpec.setSpecUrl(os.path.join(self.workDir,
                                                    'testWhitelist.spec'))
        self.whitelistSpec.taskIterator().next().data.constraints.sites.whitelist = ['SiteB']
        self.blacklistSpec.data._internal_name = 'whitelistlistSpec'
        self.whitelistSpec.save(self.whitelistSpec.specUrl())

        # Create queues
        self.globalQueue = globalQueue(CacheDir = self.workDir,
                                       NegotiationTimeout = 0,
                                       QueueURL = 'global.example.com')
#        self.midQueue = WorkQueue(SplitByBlock = False, # mid-level queue
#                            PopulateFilesets = False,
#                            ParentQueue = self.globalQueue,
#                            CacheDir = None)
        # ignore mid queue as it causes database duplication's
        self.localQueue = localQueue(ParentQueue = self.globalQueue,
                                     CacheDir = self.workDir,
                                     ReportInterval = 0,
                                     QueueURL = "local.example.com")
        # standalone queue for unit tests
        self.queue = WorkQueue(CacheDir = self.workDir)

        # setup Mock DBS and PhEDEx
        inputDataset = Tier1ReRecoWorkload.taskIterator().next().inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        mockDBS = MockDBSReader('http://example.com', dataset)
        for queue in (self.queue, self.localQueue, self.globalQueue):
            queue.dbsHelpers['http://example.com'] = mockDBS
            queue.dbsHelpers['http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'] = mockDBS
            queue.phedexService = MockPhedexService(dataset)


    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)


    def testProduction(self):
        """
        Enqueue and get work for a production WMSpec.
        """
        specfile = self.spec.specUrl()
        numBlocks = 2
        njobs = [1] * numBlocks # array of jobs per block
        total = sum(njobs)

        # Queue Work & check accepted
        for _ in range (0, numBlocks):
            self.queue.queueWork(specfile)
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
            self.queue.queueWork(self.spec.specUrl())

        # priority change
        self.queue.setPriority(50, self.spec.name())
        self.assertRaises(RuntimeError, self.queue.setPriority, 50, 'blahhhhh')

        # claim all work
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), numBlocks)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))


    def testProcessing(self):
        """
        Enqueue and get work for a processing WMSpec.
        """
        specfile = self.processingSpec.specUrl()
        njobs = [1, 1] # array of jobs per block
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
        """
        Black & White list functionality
        """
        specfile = self.blacklistSpec.specUrl()
        njobs = [5, 10] # array of jobs per block
        numBlocks = len(njobs)
        total = sum(njobs)

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numBlocks, len(self.queue))
        self.queue.updateLocationInfo()

        #In blacklist (SiteA)
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0)

        # copy block over to SiteB (all dbsHelpers point to same instance)
        fakeDBS = self.queue.dbsHelpers['http://example.com']
        for block in fakeDBS.locations:
            if block.endswith('1'):
                fakeDBS.locations[block] = ['SiteA', 'SiteB', 'SiteAA']
        self.queue.phedexService.locations.update(fakeDBS.locations)
        self.queue.updateLocationInfo()

        # SiteA still blacklisted for all blocks
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0)
        # SiteB can run all blocks now
        work = self.queue.getWork({'SiteB' : total})
        self.assertEqual(len(work), 2)

        # Test whitelist stuff
        specfile = self.whitelistSpec.specUrl()
        njobs = [5, 10] # array of jobs per block
        numBlocks = len(njobs)
        total = sum(njobs)

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numBlocks, len(self.queue))
        self.queue.updateLocationInfo()

        # Only SiteB in whitelist
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0)

        # Site B can run 
        work = self.queue.getWork({'SiteB' : total, 'SiteAA' : total})
        self.assertEqual(len(work), 2)


    def testQueueChaining(self):
        """
        Chain WorkQueues, pull work down and verify splitting
        """
        self.assertEqual(0, len(self.globalQueue))
        # check no work in local queue
        self.assertEqual(0, len(self.localQueue.getWork({'SiteA' : 1000})))
        # Add work to top most queue
        self.globalQueue.queueWork(self.processingSpec.specUrl())
        self.assertEqual(1, len(self.globalQueue))

        # check work isn't passed down to site without subscription
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}), 0)

        # put at correct site
        self.globalQueue.updateLocationInfo()

        # check work isn't passed down to the wrong agent
        work = self.localQueue.getWork({'SiteB' : 1000}) # Not in subscription
        self.assertEqual(0, len(work))
        self.assertEqual(1, len(self.globalQueue))

        # pull work down to the lowest queue
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}), 2)
        self.assertEqual(len(self.localQueue), 2)
        # parent state should be negotiating till we verify we have it
        self.assertEqual(len(self.globalQueue.status('Negotiating')), 1)

        # check work passed down to lower queue where it was acquired
        # work should have expanded and parent element marked as acquired
        #import pdb; pdb.set_trace()
        self.assertEqual(len(self.localQueue.getWork({'SiteA' : 1000})), 0)
        # releasing on block so need to update locations
        self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'SiteA' : 1000})
        self.assertEqual(0, len(self.localQueue))
        self.assertEqual(2, len(work))

        # mark work done & check this passes upto the top level
        self.localQueue.setStatus('Done',
                                  [str(x['element_id']) for x in work], id_type = 'id')


    def testQueueChainingNegotiationFailures(self):
        """Chain workQueues and verify status updates, negotiation failues etc"""
        # verify that negotiation failures are removed
        #self.globalQueue.flushNegotiationFailures()
        #self.assertEqual(len(self.globalQueue.status('Negotiating')), 0)
        #self.localQueue.updateParent()
        # TODO: Check status of element in global queue
        self.assertEqual(0, len(self.globalQueue))
        self.assertEqual(0, len(self.localQueue.getWork({'SiteA' : 1000})))

        # Add work to top most queue
        self.globalQueue.queueWork(self.processingSpec.specUrl())
        self.assertEqual(1, len(self.globalQueue))
        self.globalQueue.updateLocationInfo()
        # pull to local queue
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}), 2)

        # check that global reset's status if acquired status not verified
        self.assertEqual(len(self.globalQueue.status('Negotiating')), 1)
        self.assertEqual(len(self.localQueue.status('Available')), 2)
        self.assertEqual(self.globalQueue.flushNegotiationFailures(), 1)
        self.assertEqual(len(self.globalQueue.status('Available')), 3)
        # If original queue re-connects it will confirm work acquired
        # change queue name so it appears that a negotiation failure occurred
        # and 2 queues were allocated the work - ensure the loser is canceled
        myname = self.localQueue.params['QueueURL']
        self.localQueue.params['QueueURL'] = 'local2.example.com'
        self.localQueue.updateParent() # parent will fail children here
        self.localQueue.params['QueueURL'] = myname
        self.assertEqual(len(self.globalQueue.status('Available')), 1)
        self.assertEqual(len(self.globalQueue.status('Canceled')), 2)


    def testQueueChainingStatusUpdates(self):
        """Chain workQueues, pass work down and verify lifecycle"""
        self.assertEqual(0, len(self.globalQueue))
        self.assertEqual(0, len(self.localQueue.getWork({'SiteA' : 1000})))

        # Add work to top most queue
        self.globalQueue.queueWork(self.processingSpec.specUrl())
        self.assertEqual(1, len(self.globalQueue))
        self.globalQueue.updateLocationInfo()

        # pull to local queue
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}), 2)
        # Tell parent local has acquired
        self.assertEqual(self.localQueue.lastReportToParent, 0)
        before = self.localQueue.lastFullReportToParent
        self.localQueue.updateParent()
        self.assertNotEqual(before, self.localQueue.lastFullReportToParent)
        self.assertEqual(len(self.globalQueue.status('Acquired')), 1)
        self.assertEqual(len(self.globalQueue.status('Available')), 2)

        # run work
        self.globalQueue.updateLocationInfo()
        work = self.localQueue.getWork({'SiteA' : 1000})
        self.assertEqual(len(work), 2)

        # resend info
        before = self.localQueue.lastReportToParent
        self.localQueue.updateParent()
        self.assertNotEqual(before, self.localQueue.lastReportToParent)

        # finish work locally and propagate to global
        self.localQueue.doneWork([str(x['element_id']) for x in work])
        self.localQueue.updateParent()
        self.assertEqual(len(self.globalQueue.status('Done')), 3)


    def testMultiTaskProduction(self):
        """
        Test Multi top level task production spec.
        multiTaskProduction spec consist 2 top level tasks each task has event size 1000 and 2000
        respectfully  
        """
        # Basic production Spec
        spec = MultiTaskProductionWorkload
        spec.setSpecUrl(os.path.join(self.workDir, 'multiTaskProduction.spec'))
        spec.save(spec.specUrl())
        
        specfile = spec.specUrl()
        numElements = 3
        njobs = [1] * numElements # array of jobs per block
        total = sum(njobs)

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numElements, len(self.queue))

        # try to get work
        work = self.queue.getWork({'SiteA' : 0})
        self.assertEqual([], work)
        work = self.queue.getWork({'SiteA' : njobs[0]})
        self.assertEqual(len(work), 1)
        # claim all work
        work = self.queue.getWork({'SiteA' : total, 'SiteB' : total})
        self.assertEqual(len(work), numElements - 1)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))
        try:
            os.unlink(specfile)
        except OSError:
            pass

if __name__ == "__main__":
    unittest.main()
