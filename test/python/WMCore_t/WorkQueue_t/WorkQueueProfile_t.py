#!/usr/bin/env python
"""
    WorkQueue tests
"""

__revision__ = "$Id: WorkQueueProfile_t.py,v 1.2 2010/04/13 18:49:02 sryu Exp $"
__version__ = "$Revision: 1.2 $"

#setup emulator for test, this needs to be at top of the file
from WMQuality.Emulators.EmulatorSetup import emulatorSetup, deleteConfig
ConfigFile = emulatorSetup(phedex=True, dbs=True, siteDB=True, requestMgr=True)

import unittest
import cProfile
import pstats

from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WorkQueueTestCase import WorkQueueTestCase
    
class WorkQueueProfileTest(WorkQueueTestCase):
    """
    _WorkQueueTest_
    
    """
    
    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        WorkQueueTestCase.setUp(self)
        self.specGenerator = WMSpecGenerator()
        self.specs = self.createReRecoSpec(100)
        # Create queues
        self.globalQueue = globalQueue(CacheDir = "Global_Queue",
                                       NegotiationTimeout = 0,
                                       QueueURL = 'global.example.com')
             
    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)

    def createReRecoSpec(self, numOfSpec):
        specs = []    
        for i in range(numOfSpec):
            specName = "MinBiasProcessingSpec_Test_%s" % (i+1)
            specs.append(self.specGenerator.createReRecoSpec(specName, "spec"))
        return specs
    
    def testQueueElementProfile(self):
        file = 'queueElementProfile.prof'
        prof = cProfile.Profile()
        prof.runcall(self.multipleQueueWorkCall)
        prof.dump_stats(file)
        p = pstats.Stats(file)
        p.strip_dirs().sort_stats('cumulative').print_stats(10)
        p.strip_dirs().sort_stats('time').print_stats(10)

        
    def multipleQueueWorkCall(self):
        for wmspec in self.specs:
            units = self.globalQueue._splitWork(wmspec)
            with self.globalQueue.transactionContext():
                for unit in units:
                    self.globalQueue._insertWorkQueueElement(unit)
            
        
if __name__ == "__main__":
    unittest.main()
    deleteConfig(ConfigFile)