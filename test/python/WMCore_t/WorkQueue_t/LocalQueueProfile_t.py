#!/usr/bin/env python
"""
    WorkQueue tests
"""
from __future__ import absolute_import, division, print_function

import cProfile
import pstats
import shutil
import tempfile
import unittest

from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase

from WMCore.WorkQueue.WorkQueue import localQueue
from WMQuality.Emulators.RucioClient.MockRucioApi import SITES as DUMMY_SITES
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator


class LocalWorkQueueProfileTest(WorkQueueTestCase):
    """
    _WorkQueueTest_

    """

    def setUp(self):
        """
        If we dont have a wmspec file create one
        """

        WorkQueueTestCase.setUp(self)

        self.cacheDir = tempfile.mkdtemp()
        self.specGenerator = WMSpecGenerator(self.cacheDir)
        self.specs = self.createReRecoSpec(1, "file")
        # setup rucio parameters for global/local queue
        self.queueParams = {}
        self.queueParams['log_reporter'] = "lq_profile_test"
        self.queueParams['rucioAccount'] = "wma_test"
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"

        # Create queues
        self.localQueue = localQueue(DbName=self.queueDB, InboxDbName=self.queueInboxDB,
                                     NegotiationTimeout=0, QueueURL='global.example.com', CacheDir=self.cacheDir,
                                     central_logdb_url="%s/%s" % (self.couchURL, self.logDBName),
                                     **self.queueParams)


    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)
        try:
            shutil.rmtree(self.cacheDir)
            self.specGenerator.removeSpecs()
        except Exception:
            pass

    def createReRecoSpec(self, numOfSpec, kind="spec"):
        specs = []
        for i in range(numOfSpec):
            specName = "MinBiasProcessingSpec_Test_%s" % (i+1)
            specs.append(self.specGenerator.createReRecoSpec(specName, kind,
                                                             assignKwargs={'SiteWhitelist': ['T2_XX_SiteA']}))
        return specs

    def createProfile(self, name, function):
        fileName = name
        prof = cProfile.Profile()
        prof.runcall(function)
        prof.dump_stats(fileName)
        p = pstats.Stats(fileName)
        p.strip_dirs().sort_stats('cumulative').print_stats(0.1)
        p.strip_dirs().sort_stats('time').print_stats(0.1)
        p.strip_dirs().sort_stats('calls').print_stats(0.1)
        p.strip_dirs().sort_stats('name').print_stats(10)

    def testGetWorkLocalQueue(self):
        i = 0
        for spec in self.specs:
            i += 1
            specName = "MinBiasProcessingSpec_Test_%s" % i
            self.localQueue.queueWork(spec, specName, team="A-team")
        self.localQueue.updateLocationInfo()
        self.createProfile('getWorkProfile.prof', self.localQueueGetWork)

    def localQueueGetWork(self):
        siteJobs = {}
        for site in DUMMY_SITES:
            siteJobs[site] = 100000
        self.localQueue.getWork(siteJobs, {})


if __name__ == "__main__":
    unittest.main()
