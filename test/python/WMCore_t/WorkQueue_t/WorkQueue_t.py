#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueue_t.py,v 1.17 2009/11/12 16:43:33 swakef Exp $"
__version__ = "$Revision: 1.17 $"

import unittest
import os

from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask

from WorkQueueTestCase import WorkQueueTestCase

# NOTE: All queues point to the same database backend
# Thus total element counts etc count elements in all queues


def createSpec(name, path, dataset = None,
               blacklist = None, whitelist = None):
    """
    create a wmspec object and save to disk
    """
    wmspec = newWorkload(name)
    wmspec.data.owner = 'WorkQueueTest'
    task = makeWMTask('task1')
    if dataset:
        task.addInputDataset(**dataset)
        task.setSplittingAlgorithm("FileBased", size = 1)

        #FixMe? need setter for blocklist and whitelist
        if blacklist:
            task.data.constraints.sites.blacklist = blacklist
        if whitelist:
            task.data.constraints.sites.whitelist = whitelist
        wmspec.data.dbs = 'http://example.com'
    else:
        task.setSplittingAlgorithm("EventBased", size = 100)
        #FIXME need to add WMSpec to save total event properly for production j
        task.addProduction(totalevents = 1000)
    wmspec.addTask(task)
    wmspec.setSpecUrl(path)
    wmspec.save(path)



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

    def getDatasetInfo(self, dataset):
        """Dataset summary"""
        result = {}
        result['number_of_events'] = sum([x['NumEvents'] for x in self.blocks[dataset]])
        result['number_of_files'] = sum([x['NumFiles'] for x in self.blocks[dataset]])
        result['path'] = dataset
        return result

#TODO: This is horrible and needs to be replaced
class MockPhedexService:
    """
    TODO: Move to a proper mocking libs for this
    """
    def __init__(self):
        self.dbs = MockDBSReader('')
        self.locations = {'/fake/test/RAW#1' : ['SiteA'],
                '/fake/test/RAW#2' : ['SiteA', 'SiteB']}

    def getReplicaInfoForBlocks(self, **args):
        """
        Where are blocks located
        """
        #blocks = [ {'bytes' : bytes, 'files' : files, 'name' : name      ]
        data = {"phedex":{"request_timestamp":1254762796.13538,
                          "block" : [{"files":"5", "name": '/fake/test/RAW#1',
                                      'replica' : [{'se':'SiteA'}]},
                                     {"files":"10", "name": '/fake/test/RAW#2',
                                      'replica' : [{'se':'SiteA'}, {'se':'SiteB'}]},
                                    ]
                          }
        }
        return data

    def subscriptions(self, **args):
        """
        Where is data subscribed - for now just replicate blockreplicas
        """
        if args.has_key('dataset') and args['dataset']:
            data = {'phedex' : {"request_timestamp" : 1254850198.15418,
                                'dataset' : [{'name' : '/fake/test/RAW', 'files' : 5,
                                              'subscription' : [{'node': 'SiteA', 'custodial': 'n', 'suspend_until': None,
                                                                 'level': 'dataset', 'move': 'n', 'request': '47983',
                                                                 'time_created': '1232989000', 'priority': 'low',
                                                                 'time_update': None, 'node_id': '781',
                                                                 'suspended': 'n', 'group': None}
                                                                ]
                                              }]
                                }
                    }
            return data
        else:
            data = {"phedex":{"request_timestamp":1254920053.14921,
                              "dataset":[{"bytes":"10438786614", "files":"10", "is_open":"n",
                                          "name":"/fake/test/RAW",
                                          "block":[{"bytes":"10438786614", "files":"5", "is_open":"n",
                                                    "name":"/fake/test/RAW#1",
                                                    "id":"454370", "subscription"
                                                                                :[ {'node' : x } for x in self.locations['/fake/test/RAW#1']]
                                                                                #{"priority":"normal", "request":"51253", "time_created":"1245165314",
                                                                                #   "move":"n", "suspend_until":None, "node":"SiteA",
                                                                                #   "time_update":"1228905272", "group":None, "level":"block",
                                                                                #   "node_id":"641", "custodial":"n", "suspended":"n"}]
                                                    },
                                                     {"bytes":"10438786614", "files":"10", "is_open":"n",
                                                    "name":"/fake/test/RAW#2",
                                                    "id":"454370", "subscription"
                                                                                :[ {'node' : x } for x in self.locations['/fake/test/RAW#2']]
                                                                                #{"priority":"normal", "request":"51253", "time_created":"1245165314",
                                                                                #   "move":"n", "suspend_until":None, "node":"SiteA",
                                                                                #   "time_update":"1228905272", "group":None, "level":"block",
                                                                                #   "node_id":"641", "custodial":"n", "suspended":"n"}]
                                                    }]
                                        }], "instance":"prod"
                                }
                    }
            return data

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
        self.blacklistSpecName = 'testBlacklist'
        self.whitelistSpecName = 'testWhitelist'
        self.processingSpecFile = os.path.join(os.getcwd(),
                                            self.processingSpecName)
        self.blacklistSpecFile = os.path.join(os.getcwd(),
                                            self.blacklistSpecName)
        self.whitelistSpecFile = os.path.join(os.getcwd(),
                                            self.whitelistSpecName)

        dataset = {'primary':'fake', 'processed':'test', 'tier':'RAW',
                   'dbsurl':'http://example.com', 'totalevents':10000}

        createSpec(self.processingSpecName,
                   self.processingSpecFile, dataset)
        createSpec(self.blacklistSpecName,
                   self.blacklistSpecFile, dataset,
                   blacklist = ['SiteA'])
        createSpec(self.whitelistSpecName,
                   self.whitelistSpecFile, dataset,
                   whitelist = ['SiteB'])

        #self.globalQueue = WorkQueue(SplitByBlock = False, # Global queue
        #                             PopulateFilesets = False,
        #                             CacheDir = None)
        self.globalQueue = globalQueue(CacheDir = None, NegotiationTimeout = 0)
#        self.midQueue = WorkQueue(SplitByBlock = False, # mid-level queue
#                            PopulateFilesets = False,
#                            ParentQueue = self.globalQueue,
#                            CacheDir = None)
        # ignore mid queue as it causes database duplication's
#        self.localQueue = WorkQueue(SplitByBlock = True, # local queue
#                            ParentQueue = self.globalQueue,
#                            CacheDir = None)
        self.localQueue = localQueue(ParentQueue = self.globalQueue,
                                     CacheDir = None,
                                     ReportInterval = 0,
                                     QueueURL = "local.example.com")
        # standalone queue for unit tests
        self.queue = WorkQueue(CacheDir = None)
        mockDBS = MockDBSReader('http://example.com')
        for queue in (self.queue, self.localQueue, self.globalQueue):
            queue.dbsHelpers['http://example.com'] = mockDBS
            queue.phedexService = MockPhedexService()


    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)

        for f in (self.specFile, self.processingSpecFile,
                  self.blacklistSpecFile, self.whitelistSpecFile):
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
        #TODO: This tests nothing! Fix.

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
        Enqueue and get work for a processing WMSpec.
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
        """
        Black & White list functionality
        """
        specfile = self.blacklistSpecFile
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

        fakeDBS = self.queue.dbsHelpers['http://example.com']
        fakeDBS.locations['/fake/test/RAW#1'] = ['SiteA', 'SiteB']
        self.queue.phedexService.locations.update(fakeDBS.locations)
        self.queue.updateLocationInfo()

        # SiteA still blacklisted for all blocks
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0)
        # SiteB can run all blocks now
        work = self.queue.getWork({'SiteB' : total})
        self.assertEqual(len(work), 2)

        # Test whitelist stuff
        specfile = self.whitelistSpecFile
        njobs = [5, 10] # array of jobs per block
        numBlocks = len(njobs)
        total = sum(njobs)

        # Queue Work & check accepted
        self.queue.queueWork(specfile)
        self.assertEqual(numBlocks, len(self.queue))

        # Only SiteB in whitelist
        work = self.queue.getWork({'SiteA' : total})
        self.assertEqual(len(work), 0) # Fail here till whitelist works

        # Site B can run
        work = self.queue.getWork({'SiteB' : total})
        self.assertEqual(len(work), 2)


    def testQueueChaining(self):
        """
        Chain WorkQueues, pull work down and verify splitting
        """
        self.assertEqual(0, len(self.globalQueue))
        # check no work in local queue
        self.assertEqual(0, len(self.localQueue.getWork({'SiteA' : 1000})))
        # Add work to top most queue
        self.globalQueue.queueWork(self.processingSpecFile)
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
                                  [str(x['subscription_id']) for x in work])


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
        self.globalQueue.queueWork(self.processingSpecFile)
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
        self.globalQueue.queueWork(self.processingSpecFile)
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

        # run work
        self.globalQueue.updateLocationInfo()
        work = self.localQueue.getWork({'SiteA' : 1000})
        self.assertEqual(len(work), 2)

        # resend info
        before = self.localQueue.lastReportToParent
        self.localQueue.updateParent()
        self.assertNotEqual(before, self.localQueue.lastReportToParent)

        # finish work locally and propagate to global
        self.localQueue.doneWork(*[str(x['subscription_id']) for x in work])
        self.localQueue.updateParent()
        self.assertEqual(len(self.globalQueue.status('Done')), 3)


if __name__ == "__main__":
    unittest.main()
