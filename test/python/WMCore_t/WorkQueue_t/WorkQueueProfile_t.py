#!/usr/bin/env python
"""
    WorkQueue tests
"""




#setup emulator for test, this needs to be at top of the file
from WMQuality.Emulators.EmulatorSetup import emulatorSetup, deleteConfig
ConfigFile = emulatorSetup(phedex=True, dbs=True, siteDB=True, requestMgr=True)

import tempfile
import unittest
import cProfile
import pstats
from WMQuality.Emulators.DataBlockGenerator import Globals
from WMQuality.Emulators.DataBlockGenerator.Globals import GlobalParams
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
        
        Warning: For the real profiling test including 
        spec generation. need to use real spec instead of 
        using emulator generated spec which doesn't include
        couchDB access and cmssw access  
        """
        WorkQueueTestCase.setUp(self)
        self.specGenerator = WMSpecGenerator()
        self.specs = self.createReRecoSpec(10)
        
        self.cacheDir = tempfile.mkdtemp() 
        # Create queues
        self.globalQueue = globalQueue(CacheDir = self.cacheDir,
                                       NegotiationTimeout = 0,
                                       QueueURL = 'global.example.com')
        
        
    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)
        try:
            shutil.rmtree( self.cacheDir )
            self.specGenerator.removeSpecs()
        except:
            pass
        
    def createReRecoSpec(self, numOfSpec, type = "spec"):
        specs = []    
        for i in range(numOfSpec):
            specName = "MinBiasProcessingSpec_Test_%s" % (i+1)
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
        for wmspec in self.specs:
            units = self.globalQueue._splitWork(wmspec)
            with self.globalQueue.transactionContext():
                for unit in units:
                    self.globalQueue._insertWorkQueueElement(unit)
                             
        
if __name__ == "__main__":
    unittest.main()
    deleteConfig(ConfigFile)