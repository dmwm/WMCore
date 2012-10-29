#!/usr/bin/env python
"""
    WorkQueue tests
"""

import tempfile
import unittest
import cProfile
import pstats
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.WorkQueue.WorkQueue import globalQueue
from WorkQueueTestCase import WorkQueueTestCase
from WMCore.Services.EmulatorSwitch import EmulatorHelper

class WorkQueueProfileTest(WorkQueueTestCase):
    """
    _WorkQueueTest_

    """


    def setUp(self):
        """
        If we dont have a wmspec file create one

        Warning: For the real profiling test including
        spec generation. need to use real spec instead of
        using emulator generated spec which doesn't include
        couchDB access and cmssw access
        """
        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                                    siteDB = True, requestMgr = True)
        WorkQueueTestCase.setUp(self)

        self.cacheDir = tempfile.mkdtemp()
        self.specGenerator = WMSpecGenerator(self.cacheDir)
        self.specNamePrefix = "TestReReco_"
        self.specs = self.createReRecoSpec(5, "file")
        # Create queues
        self.globalQueue = globalQueue(DbName = self.globalQDB,
                                       InboxDbName = self.globalQInboxDB,
                                       NegotiationTimeout = 0)


    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)
        try:
            self.specGenerator.removeSpecs()
        except:
            pass
        EmulatorHelper.resetEmulators()

    def createReRecoSpec(self, numOfSpec, type = "spec"):
        specs = []
        for i in range(numOfSpec):
            specName = "%s%s" % (self.specNamePrefix, (i+1))
            specs.append(self.specGenerator.createReRecoSpec(specName, type))
        return specs

    def createProfile(self, name, function):
        file = name
        prof = cProfile.Profile()
        prof.runcall(function)
        prof.dump_stats(file)
        p = pstats.Stats(file)
        p.strip_dirs().sort_stats('cumulative').print_stats(0.1)
        p.strip_dirs().sort_stats('time').print_stats(0.1)
        p.strip_dirs().sort_stats('calls').print_stats(0.1)
        #p.strip_dirs().sort_stats('name').print_stats(10)

    def testQueueElementProfile(self):
        self.createProfile('queueElementProfile.prof',
                           self.multipleQueueWorkCall)

    def multipleQueueWorkCall(self):
        i = 0
        for wmspec in self.specs:
            i += 1
            self.globalQueue.queueWork(wmspec, self.specNamePrefix + str(i), 'test_team')

if __name__ == "__main__":
    unittest.main()
