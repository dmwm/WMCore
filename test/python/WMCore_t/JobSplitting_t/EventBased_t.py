#!/usr/bin/env python
"""
_EventBased_

Event based splitting test

"""
__revision__ = "$Id: EventBased_t.py,v 1.3 2008/10/29 13:21:49 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"

from sets import Set
import unittest
import logging
import os
import commands
import random
import datetime
import math
import hotshot, hotshot.stats
#logging.getLogger().setLevel(logging.DEBUG)
#logging.getLogger().addHandler(logging.StreamHandler())

from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Workflow import Workflow



class EventBasedTest(unittest.TestCase):
    """
    _EventBasedTest_

    Test event based splitting

    """

    def setUp(self):
        """set up tests"""
        self.statsFile = "/tmp/WMCore.JobSplitting.EventBased_t.stats"
        self.fileset1 = Fileset(name='EventBasedFiles1')
        for i in range(0, 10):
            self.fileset1.addFile(
                File("/store/MultipleFileSplit%s.root" % i, # lfn
                     1000,   # size
                     100,   # events
                     10 + i, # run
                     12312   # lumi
                     )
                )

        self.fileset2 = Fileset(name='EventBasedFiles2')
        self.fileset2.addFile(
            File("/store/SingleFileSplit.root" , # lfn
                 1000,   # size
                 100,   # events
                 10, # run
                 12312   # lumi
                 )
            )

        work = Workflow()
        self.subscription1 = Subscription(
            fileset = self.fileset1,
            workflow = work,
            split_algo = 'EventBased',
            type = "Processing")
        self.subscription2 = Subscription(
            fileset = self.fileset2,
            workflow = work,
            split_algo = 'EventBased',
            type = "Processing")



    def tearDown(self):
        """cleanup"""
        if os.path.exists(self.statsFile):
            os.remove(self.statsFile)




    def testA(self):
        """
        pedantic checks of algorithm with single file
        """
        #prof = hotshot.Profile(self.statsFile)
        #prof.start()

        splitter = SplitterFactory()
        jobfactory = splitter(self.subscription2)

        #  //
        # // test 1: split into single job with exact events
        #//
        jobs = jobfactory(events_per_job = 100)
        self.assertEqual(len(jobs.jobs), 1)
        job = jobs.jobs.pop()
        self.assertEqual(job.getFiles(type='lfn'),['/store/SingleFileSplit.root'])
        self.assertEqual(job.mask.getMaxEvents(), 100)
        self.assertEqual(job.mask['FirstEvent'], 0)


        #  //
        # //  test 2: split into single job with >> events in file
        #//
        jobs = jobfactory(events_per_job = 1000)
        self.assertEqual(len(jobs.jobs), 1)
        job = jobs.jobs.pop()
        self.assertEqual(job.getFiles(type='lfn'),['/store/SingleFileSplit.root'])
        self.assertEqual(job.mask.getMaxEvents(), 1000)
        self.assertEqual(job.mask['FirstEvent'], 0)


        #  //
        # // test3: two job split
        #//

        jobs = jobfactory(events_per_job = 50)
        self.assertEqual(len(jobs.jobs), 2)
        for job in jobs.jobs:
            # 1 file per job, should be same LFN
            self.assertEqual(len(job.file_set), 1)
            self.assertEqual(job.getFiles(type='lfn'), ['/store/SingleFileSplit.root'])
            self.assertEqual(job.mask.getMaxEvents(), 50)
            self.failUnless(job.mask['FirstEvent'] in [0, 50])

        #  //
        # // test 4
        #//
        jobs = jobfactory(events_per_job = 99)
        self.assertEqual(len(jobs.jobs), 2)
        for job in jobs.jobs:
            # 1 file per job, should be same LFN
            self.assertEqual(len(job.file_set), 1)
            self.assertEqual(job.getFiles(type='lfn'), ['/store/SingleFileSplit.root'])
            self.assertEqual(job.mask.getMaxEvents(), 99)
            self.failUnless(job.mask['FirstEvent'] in [0, 99])






        #prof.stop()
        #stats = hotshot.stats.load(self.statsFile)
        #stats.strip_dirs()
        #stats.sort_stats('time', 'calls')
        #stats.print_stats(10)



    def testB(self):
        """multi file tests"""

        splitter = SplitterFactory()
        jobfactory = splitter(self.subscription1)

        #  //
        # // test 1: 1 job per file
        #//
        jobs = jobfactory(events_per_job = 100)
        self.assertEqual(len(jobs.jobs), 10)
        for job in jobs.jobs:
            self.failUnless(len(job.getFiles(type='lfn')) == 1)
            self.failUnless(
                job.mask.getMaxEvents() == 100
                )
            self.failUnless(
                job.mask['FirstEvent'] == 0
                )


        #  //
        # // test 2: 2 jobs per file
        #//
        jobs = jobfactory(events_per_job = 50)
        self.assertEqual(len(jobs.jobs), 20)
        for job in jobs.jobs:
            self.failUnless(len(job.getFiles(type='lfn')) == 1)
            self.failUnless(
                job.mask.getMaxEvents() == 50
                )
            self.failUnless(
                job.mask['FirstEvent'] in (0, 50)
                )
        #  //
        # // test 3:   jobs crossing file boundaries
        #//
        jobs = jobfactory(events_per_job = 125)
        self.assertEqual(len(jobs.jobs), 8)

        firstEvents = []
        for job in jobs.jobs:
            self.failUnless(len(job.getFiles(type='lfn')) in (1,2))
            self.failUnless(
                job.mask.getMaxEvents() == 125
                )
            firstEvents.append(job.mask['FirstEvent'])

        self.failUnless(firstEvents.count(0) == 2)
        self.failUnless(firstEvents.count(25) == 2)
        self.failUnless(firstEvents.count(50) == 2)
        self.failUnless(firstEvents.count(75) == 2)







if __name__ == '__main__':

    unittest.main()
