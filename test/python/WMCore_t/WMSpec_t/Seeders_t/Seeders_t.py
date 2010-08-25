#!/usr/bin/env python

import unittest


from WMCore.WMSpec.Seeders.SeederManager import SeederManager
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper, makeWMTask

from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.JobPackage import JobPackage




class SeederTest(unittest.TestCase):



    def setUp(self):
        """
        _setUp_

        """

        self.manager = SeederManager()

        self.seedlistForRandom = {
            "simMuonRPCDigis": None,
            "simEcalUnsuppressedDigis": None,
            "simCastorDigis": None,
            "generator": None,
            "simSiStripDigis": None,
            "LHCTransport": None,
            "mix": None,
            "simHcalUnsuppressedDigis": None,
            "theSource": None,
            "simMuonCSCDigis": None,
            "VtxSmeared": None,
            "g4SimHits": None,
            "simSiPixelDigis": None,
            "simMuonDTDigis": None,
            "evtgenproducer": None
            }


        return

    def tearDown(self):
        """
        _tearDown_

        """
        #Do nothing

        return

    def oneHundredFiles(self, splittingAlgo = "EventBased", jobType = "Processing"):
        """
        _oneHundredFiles_
        
        Generate a WMBS data stack representing 100 files for job splitter
        testing
        
        """
        fileset1 = Fileset(name='EventBasedFiles1')
        for i in range(0, 100):
            fileset1.addFile(
            File("/store/MultipleFileSplit%s.root" % i, # lfn
                 1000,   # size
                 100,   # events
                 10 + i, # run
                 12312   # lumi
                 )
            )

        work = Workflow()
        subscription1 = Subscription(
            fileset = fileset1,
            workflow = work,
            split_algo = splittingAlgo,
            type = jobType)
        splitter = SplitterFactory()
        jobfactory = splitter(subscription1)
        jobs = jobfactory(events_per_job = 100)
        #for jobGroup in jobs:
        #    yield jobGroup




        self.manager.addSeeder("RandomSeeder", **self.seedlistForRandom)
        self.manager.addSeeder("RunAndLumiSeeder")

        return jobs



    def testSimpleFiles(self):
        """
        _testSimpleFiles_

        
        test using one hundred files that we can save the attributes in a job
        """
        jobs = self.oneHundredFiles()

        for jobGrp in jobs:
            self.manager(jobGrp)


        for jobGrp in jobs:
            count = 0
            for job in jobGrp.jobs:
                conf = job.getBaggage()
                self.assertEqual(hasattr(conf.RandomSeeder.evtgenproducer, 'initialSeed'), True)
                self.assertEqual(hasattr(conf.RandomSeeder.generator, 'initialSeed'), True)
                self.assertEqual(job["mask"]["FirstLumi"], count%11)
                self.assertEqual(job["mask"]["FirstRun"],  (count/11) + 1)
                count += 1

        return


    def testWMTask(self):
        """
        _testWMTask_
        
        Test whether or not we can read the seeder parameters out of a WMTask.
        """

        task1 = makeWMTask("task1")

        task1.data.section_("seeders")
        task1.data.seeders.section_("RandomSeeder")
        task1.data.seeders.section_("RunAndLumiSeeder")
        task1.data.seeders.RandomSeeder.simMuonRPCDigis            = None
        task1.data.seeders.RandomSeeder.simEcalUnsuppressedDigis   = None
        task1.data.seeders.RandomSeeder.simCastorDigis             = None
        task1.data.seeders.RandomSeeder.generator                  = None 
        task1.data.seeders.RandomSeeder.simSiStripDigis            = None
        task1.data.seeders.RandomSeeder.LHCTransport               = None
        task1.data.seeders.RandomSeeder.mix                        = None
        task1.data.seeders.RandomSeeder.simHcalUnsuppressedDigis   = None
        task1.data.seeders.RandomSeeder.theSource                  = None
        task1.data.seeders.RandomSeeder.simMuonCSCDigis            = None
        task1.data.seeders.RandomSeeder.VtxSmeared                 = None
        task1.data.seeders.RandomSeeder.g4SimHits                  = None
        task1.data.seeders.RandomSeeder.simSiPixelDigis            = None
        task1.data.seeders.RandomSeeder.simMuonDTDigis             = None
        task1.data.seeders.RandomSeeder.evtgenproducer             = None
        task1.data.seeders.RunAndLumiSeeder.lumi_per_run           = 5

        manager = SeederManager(task = task1)

        jobs = self.oneHundredFiles()

        for jobGrp in jobs:
            manager(jobGrp)

        for jobGrp in jobs:
            count = 0
            for job in jobGrp.jobs:
                conf = job.getBaggage()
                self.assertEqual(hasattr(conf.RandomSeeder.evtgenproducer, 'initialSeed'), True)
                self.assertEqual(hasattr(conf.RandomSeeder.generator, 'initialSeed'), True)
                self.assertEqual(job["mask"]["FirstLumi"], count%6)
                self.assertEqual(job["mask"]["FirstRun"],  (count/6) + 1)
                count += 1

        return

if __name__ == '__main__':
    unittest.main()





