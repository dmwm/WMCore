#!/usr/bin/env python
"""
    WorkQueue tests
"""




import unittest
import os

from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase
from WMCore_t.WMSpec_t.samples.BasicProductionWorkload \
                                    import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload \
                                import workload as MultiTaskProductionWorkload
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.StdSpecs.ReReco import getTestArguments
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
from WMCore_t.WorkQueue_t.MockPhedexService import MockPhedexService

class fakeSiteDB:
    """Fake sitedb interactions"""
    def phEDExNodetocmsName(self, node):
        """strip buffer/mss etc"""
        return node.replace('_MSS',
                            '').replace('_Buffer',
                                        '').replace('_Export', '')

# NOTE: All queues point to the same database backend
# Thus total element counts etc count elements in all queues
rerecoArgs = getTestArguments()
#update to not connect couch
rerecoArgs.update({
    "CouchUrl": None,
    "CouchDBName": None,
    })

class TestReRecoFactory(ReRecoWorkloadFactory):
    """Override bits that talk to cmsssw"""
    
    def getOutputModuleInfo(self, configUrl, scenarioName, scenarioFunc,
                            scenarioArgs):
        return {}

    #TODO: Remove this when each queue can be isolated (i.e. separate db's)
    def setReRecoPolicy(self, workload, splitAlgo, splitAgrs):
        """Force DatasetBlock till test cases can handle multiple queues
           with Block splitting at global level"""
        ReRecoWorkloadFactory.setReRecoPolicy(self, workload,
                                              splitAlgo, splitAgrs)
        workload.setStartPolicy("DatasetBlock")

def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable-msg=E1101,E1103
    return wmspec.taskIterator().next()

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

        rerecoFactory = TestReRecoFactory()
        # Sample Tier1 ReReco spec
        self.processingSpec = rerecoFactory('testProcessing', rerecoArgs)
        self.processingSpec.setSpecUrl(os.path.join(self.workDir,
                                                    'testProcessing.spec'))
        self.processingSpec.save(self.processingSpec.specUrl())

        # ReReco spec with blacklist
        self.blacklistSpec = rerecoFactory('blacklistSpec', rerecoArgs)
        self.blacklistSpec.setSpecUrl(os.path.join(self.workDir,
                                                    'testBlacklist.spec'))
        getFirstTask(self.blacklistSpec).data.constraints.sites.blacklist = ['SiteA']
        self.blacklistSpec.save(self.blacklistSpec.specUrl())

        # ReReco spec with whitelist
        self.whitelistSpec = rerecoFactory('whitelistlistSpec', rerecoArgs)
        self.whitelistSpec.setSpecUrl(os.path.join(self.workDir,
                                                    'testWhitelist.spec'))
        getFirstTask(self.whitelistSpec).data.constraints.sites.whitelist = ['SiteB']
        self.whitelistSpec.save(self.whitelistSpec.specUrl())

        # setup Mock DBS and PhEDEx
        inputDataset = getFirstTask(self.processingSpec).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        mockDBS = MockDBSReader('http://example.com', dataset)
        dbsHelpers = {'http://example.com' : mockDBS,
                      'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet' : mockDBS,
                      }

        # Create queues
        self.globalQueue = globalQueue(CacheDir = self.workDir,
                                       NegotiationTimeout = 0,
                                       QueueURL = 'global.example.com',
                                       DBSReaders = dbsHelpers)
#        self.midQueue = WorkQueue(SplitByBlock = False, # mid-level queue
#                            PopulateFilesets = False,
#                            ParentQueue = self.globalQueue,
#                            CacheDir = None)
        # ignore mid queue as it causes database duplication's
        self.localQueue = localQueue(ParentQueue = self.globalQueue,
                                     CacheDir = self.workDir,
                                     ReportInterval = 0,
                                     QueueURL = "local.example.com",
                                     DBSReaders = dbsHelpers)
        self.localQueue2 = localQueue(ParentQueue = self.globalQueue,
                                     CacheDir = self.workDir,
                                     ReportInterval = 0,
                                     QueueURL = "local2.example.com",
                                     DBSReaders = dbsHelpers)

        # standalone queue for unit tests
        self.queue = WorkQueue(CacheDir = self.workDir,
                               DBSReaders = dbsHelpers)

        for queue in (self.queue, self.localQueue,
                      self.localQueue2, self.globalQueue):
            queue.phedexService = MockPhedexService(dataset)
            queue.SiteDB = fakeSiteDB()
            
    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)


    def testProduction(self):
        """
        Enqueue and get work for a production WMSpec.
        """
        specfile = self.spec.specUrl()
        numUnit = 2
        jobSlot = [10] * numUnit # array of jobs per block
        total = sum(jobSlot)
        
        for _ in range(numUnit):
            self.queue.queueWork(specfile)
        self.assertEqual(20, len(self.queue))

        # try to get work
        work = self.queue.getWork({'SiteA' : 0})
        self.assertEqual([], work)
        work = self.queue.getWork({'SiteA' : jobSlot[0]})
        self.assertEqual(len(work), 10)
        # claim all work
        work = self.queue.getWork({'SiteA' : total, 'SiteB' : total})
        self.assertEqual(len(work), 10)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))


    def testPriority(self):
        """
        Test priority change functionality
        """
        totalJobs = 10
        jobSlot = 10

        self.queue.queueWork(self.spec.specUrl())

        # priority change
        self.queue.setPriority(50, self.spec.name())
        self.assertRaises(RuntimeError, self.queue.setPriority, 50, 'blahhhhh')

        # claim all work
        work = self.queue.getWork({'SiteA' : jobSlot})
        self.assertEqual(len(work), totalJobs)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : jobSlot})))


    def testProcessing(self):
        """
        Enqueue and get work for a processing WMSpec.
        """
        specfile = self.processingSpec.specUrl()
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
        
        fakeDBS = self.queue.dbsHelpers['http://example.com']
        for block in fakeDBS.locations:
            if block.endswith('1'):
                fakeDBS.locations[block] = ['SiteA', 'SiteB', 'SiteAA']
        self.queue.phedexService.locations.update(fakeDBS.locations)
        self.queue.updateLocationInfo()

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numBlocks, len(self.queue))

        # Only SiteB in whitelist
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0)

        # Site B can run
        #import pdb
        #pdb.set_trace() 
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


    def testMultipleQueueChaining(self):
        """
        Chain workQueues and verify status updates, negotiation failues etc
        """
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
        # no work available for queue2 - Negotiating
        self.assertEqual(self.localQueue2.pullWork({'SiteA' : 1000}), 0)
        # queue1 hasn't claimed work so reset element to Available
        self.assertEqual(self.globalQueue.flushNegotiationFailures(), 1)
        # work still available in queue1 until it contacts parent
        self.assertEqual(len(self.globalQueue.status('Available')), 3)

        # queue2 pull available work
        self.assertEqual(self.localQueue2.pullWork({'SiteA' : 1000}), 2)
        self.assertEqual(len(self.globalQueue.status('Negotiating')), 1)
        self.assertEqual(len(self.localQueue.status('Available')), 4)
        self.localQueue2.updateParent() # queue2 claims work
        self.assertEqual(len(self.globalQueue.status('Negotiating')), 0)
        self.assertEqual(len(self.globalQueue.status('Acquired')), 1)
        self.assertEqual(len(self.localQueue.status('Available')), 4)

        # queue1 calls back to parent and find work claimed by queue2
        self.localQueue.updateParent()
        self.assertEqual(len(self.globalQueue.status('Acquired')), 1)
        # As all queues share the same db - all elements will be canceled
        # as delete is keyed on parent id and no elements will be available
        # in real life - 1 element will be canceled and 2 will be available
        self.assertEqual(len(self.localQueue.status('Canceled')), 4)
        self.assertEqual(len(self.localQueue2.status('Available')), 0)


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
        [x.update({'Id' : x['element_id'], 'PercentComplete' : 100,
                   'PercentSuccess' : 99}) for x in work]
        [self.localQueue.setProgress(x) for x in work]
        elements = self.localQueue.status('Done')
        self.assertEqual(len(elements), len(work))
        self.assertEqual([x['PercentComplete'] for x in elements],
                         [100] * len(work))
        self.assertEqual([x['PercentSuccess'] for x in elements],
                         [99] * len(work))
        self.localQueue.updateParent(skipWMBS = True) # will delete elements from local
        elements = self.globalQueue.status('Done')
        self.assertEqual(len(elements), 1)
        self.assertEqual([x['PercentComplete'] for x in elements], [100])
        self.assertEqual([x['PercentSuccess'] for x in elements], [99])


    def testMultiTaskProduction(self):
        """
        Test Multi top level task production spec.
        multiTaskProduction spec consist 2 top level tasks each task has event size 1000 and 2000
        respectfully  
        """
        #TODO: needs more rigorous test on each element per task
        # Basic production Spec
        spec = MultiTaskProductionWorkload
        spec.setSpecUrl(os.path.join(self.workDir, 'multiTaskProduction.spec'))
        spec.save(spec.specUrl())
        
        specfile = spec.specUrl()
        numElements = 3
        njobs = [10] * numElements # array of jobs per block
        total = sum(njobs)

        # Queue Work &njobs check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numElements * 10, len(self.queue))

        # try to get work
        work = self.queue.getWork({'SiteA' : 0})
        self.assertEqual([], work)
        work = self.queue.getWork({'SiteA' : njobs[0]})
        self.assertEqual(len(work), 10)
        # claim all work
        work = self.queue.getWork({'SiteA' : total, 'SiteB' : total})
        self.assertEqual(len(work), 20)

        #no more work available
        self.assertEqual(0, len(self.queue.getWork({'SiteA' : total})))
        try:
            os.unlink(specfile)
        except OSError:
            pass


    def testTeams(self):
        """
        Team behaviour
        """
        specfile = self.spec.specUrl()
        self.globalQueue.queueWork(specfile, team = 'The A-Team')
        self.assertEqual(10, len(self.globalQueue))
        slots = {'SiteA' : 1000, 'SiteB' : 1000}

        # Can't get work for wrong team
        self.assertEqual([], self.globalQueue.getWork(slots, team = 'other'))
        # now do chain
        self.localQueue.params['Teams'] = ['other']
        self.assertEqual(self.localQueue.pullWork(slots), 0)
        # and with correct team name
        self.localQueue.params['Teams'] = ['The A-Team']
        self.assertEqual(self.localQueue.pullWork(slots), 100)
        # when work leaves the queue in the agent it doesn't care about teams
        self.assertEqual(len(self.localQueue.getWork(slots)), 100)
        self.localQueue.updateParent()
        self.assertEqual(0, len(self.globalQueue))

        # with multiple teams
        self.globalQueue.queueWork(specfile, team = 'The B-Team')
        self.globalQueue.queueWork(specfile, team = 'The C-Team')
        self.localQueue.params['Teams'] = ['The B-Team', 'The C-Team']
        self.localQueue.updateParent()
        self.assertEqual(self.localQueue.pullWork(slots), 200)
        self.localQueue.updateParent()
        self.assertEqual(len(self.localQueue.getWork(slots)), 200)


    def testGlobalBlockSplitting(self):
        """Block splitting at global level"""
        # force global queue to split work on block
        self.globalQueue.params['SplittingMapping']['DatasetBlock']['name'] = 'Block'
        self.globalQueue.params['SplittingMapping']['Block']['name'] = 'Block'
        self.globalQueue.params['SplittingMapping']['Dataset']['name'] = 'Block'

        # queue work, globally for block, pass down, report back -> complete
        totalSpec = 1
        totalBlocks = totalSpec * 2
        self.assertEqual(0, len(self.globalQueue))
        for _ in range(totalSpec):
            self.globalQueue.queueWork(self.processingSpec.specUrl())
        self.assertEqual(totalBlocks, len(self.globalQueue))

        # pull to local
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}),
                         totalBlocks)
        self.assertEqual(len(self.localQueue.status(status = 'Available')),
                         totalBlocks) # 2 in local
        self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'SiteA' : 1000, 'SiteB' : 1000})
        self.assertEqual(len(work), totalBlocks)
        # both refer to same wmspec
        self.assertEqual(work[0]['url'], work[1]['url'])
        self.localQueue.doneWork([str(x['element_id']) for x in work])
        self.localQueue.updateParent()
        # elements in local deleted at end of update, only global ones left
        self.assertEqual(len(self.localQueue.status(status = 'Done')),
                         totalBlocks)


    def testGlobalDatsetSplitting(self):
        """Dataset splitting at global level"""
        # force global queue to split work on block
        self.globalQueue.params['SplittingMapping']['DatasetBlock']['name'] = 'Dataset'
        self.globalQueue.params['SplittingMapping']['Block']['name'] = 'Dataset'
        self.globalQueue.params['SplittingMapping']['Dataset']['name'] = 'Dataset'

        # queue work, globally for block, pass down, report back -> complete
        totalSpec = 1
        totalBlocks = totalSpec * 2
        self.assertEqual(0, len(self.globalQueue))
        for _ in range(totalSpec):
            self.globalQueue.queueWork(self.processingSpec.specUrl())
        self.assertEqual(totalSpec, len(self.globalQueue))

        # pull to local
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}),
                         totalBlocks)
        self.assertEqual(len(self.localQueue.status(status = 'Available')),
                         totalBlocks) # 2 in local
        self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'SiteA' : 1000, 'SiteB' : 1000})
        self.assertEqual(len(work), totalBlocks)
        # both refer to same wmspec
        self.assertEqual(work[0]['url'], work[1]['url'])
        self.localQueue.doneWork([str(x['element_id']) for x in work])
        self.localQueue.updateParent()
        # elements in local deleted at end of update, only global ones left
        self.assertEqual(len(self.localQueue.status(status = 'Done')),
                         totalSpec)


    def testResetWork(self):
        """Reset work in global to different child queue"""
        totalBlocks = 2
        self.globalQueue.queueWork(self.processingSpec.specUrl())
        self.globalQueue.updateLocationInfo()
        self.assertEqual(self.localQueue.pullWork({'SiteA' : 1000}),
                         totalBlocks)
        self.localQueue.updateLocationInfo()
        work = self.localQueue.getWork({'SiteA' : 1000, 'SiteB' : 1000})
        self.assertEqual(len(work), totalBlocks)
        self.localQueue.updateParent()
        self.assertEqual(len(self.localQueue.status(status = 'Acquired')),
                         3) # sum of both queues

        # Re-assign work in global
        self.globalQueue.resetWork([x['id'] for x in work])
        self.localQueue.updateParent()
        # work should be canceled in local

        self.assertEqual(len(self.localQueue.status(status = 'Acquired')),
                         0)
        self.assertEqual(len(self.localQueue.status(status = 'Available')),
                         1)
        work_at_local = [x for x in self.globalQueue.status(status = 'Acquired') \
                         if x['ChildQueueUrl'] == self.localQueue.params['QueueURL']]
        self.assertEqual(len(work_at_local), 0)

        # now 2nd queue calls and acquires work
        self.assertEqual(self.localQueue2.pullWork({'SiteA' : 1000}),
                         totalBlocks)
        self.localQueue2.updateParent()

        # check work in global assigned to local2
        self.assertEqual(len(self.localQueue.status(status = 'Available')),
                         2) # work in local2
        work_at_local2 = [x for x in self.globalQueue.status(status = 'Acquired') \
                         if x['ChildQueueUrl'] == self.localQueue2.params['QueueURL']]
        self.assertEqual(len(work_at_local2), 1)


    def testCancelWork(self):
        """Cancel work"""
        self.queue.queueWork(self.processingSpec.specUrl())
        elements = len(self.queue)
        self.queue.updateLocationInfo()
        work = self.queue.getWork({'SiteA' : 1000, 'SiteB' : 1000})
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(len(self.queue.status(status='Acquired')), elements)
        ids = [x['element_id'] for x in work]
        canceled = self.queue.cancelWork(ids)
        self.assertEqual(sorted(canceled), sorted(ids))
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(len(self.queue.status(status='Canceled')), elements)

        # now cancel a request
        self.queue.queueWork(self.spec.specUrl(), request = 'Request-1')
        elements = len(self.queue)
        work = self.queue.getWork({'SiteA' : 1000, 'SiteB' : 1000})
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(len(self.queue.status(status='Acquired')), elements)
        ids = [x['element_id'] for x in work]
        canceled = self.queue.cancelWork('Request-1', id_type = 'request_name')
        self.assertEqual(canceled, 'Request-1')
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(len(self.queue.status(status='Canceled',
                                               elementIDs = ids)), elements)

if __name__ == "__main__":
    unittest.main()