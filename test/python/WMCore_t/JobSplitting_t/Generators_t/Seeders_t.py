#!/usr/bin/env python

from __future__ import division
from builtins import range

import unittest


from WMCore.JobSplitting.Generators.GeneratorManager import GeneratorManager
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

        self.manager = GeneratorManager()

        self.seedlistForRandom = {
            "simMuonRPCDigis.initialSeed": None,
            "simEcalUnsuppressedDigis.initialSeed": None,
            "simCastorDigis.initialSeed": None,
            "generator.initialSeed": None,
            "simSiStripDigis.initialSeed": None,
            "LHCTransport.initialSeed": None,
            "mix.initialSeed": None,
            "simHcalUnsuppressedDigis.initialSeed": None,
            "theSource.initialSeed": None,
            "simMuonCSCDigis.initialSeed": None,
            "VtxSmeared.initialSeed": None,
            "g4SimHits.initialSeed": None,
            "simSiPixelDigis.initialSeed": None,
            "simMuonDTDigis.initialSeed": None,
            "evtgenproducer.initialSeed": None
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
            f = File("/store/MultipleFileSplit%s.root" % i, # lfn
                 1000,   # size
                 100,   # events
                 10 + i, # run
                 12312   # lumi
                 )
            f['locations'].add("BULLSHIT")

            fileset1.addFile(f            )

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




        self.manager.addGenerator("RandomSeeder", **self.seedlistForRandom)
        self.manager.addGenerator("RunAndLumiSeeder")

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
                self.assertEqual(job["mask"]["FirstRun"],  (count // 11) + 1)
                count += 1

        return


    def testWMTask(self):
        """
        _testWMTask_

        Test whether or not we can read the seeder parameters out of a WMTask.
        Also tests RandomSeeder and RunAndLumiSeeder
        """

        task1 = makeWMTask("task1")

        randomDict = {"generator.initialSeed": None, "evtgenproducer.initialSeed": None, "MAXINT": 1}
        lumiDict   = {"lumi_per_run": 5}

        task1.addGenerator("RandomSeeder", **randomDict)
        task1.addGenerator("RunAndLumiSeeder", **lumiDict)

        manager = GeneratorManager(task = task1)

        jobs = self.oneHundredFiles()

        for jobGrp in jobs:
            manager(jobGrp)

        for jobGrp in jobs:
            count = 0
            for job in jobGrp.jobs:
                conf = job.getBaggage()
                self.assertTrue(hasattr(conf.RandomSeeder.evtgenproducer, 'initialSeed'))
                self.assertTrue(hasattr(conf.RandomSeeder.generator, 'initialSeed'))
                #self.assertEqual(job["mask"]["FirstLumi"], count%6)
                #self.assertEqual(job["mask"]["FirstRun"],  (count/6) + 1)
                x = conf.RandomSeeder.generator.initialSeed
                self.assertTrue( x > 0, "ERROR: producing negative random numbers")
                self.assertTrue( x <= 1, "ERROR: MAXINT tag failed; producing bad random number %i" %(x))
                count += 1

        return




    def testPresetSeeder(self):
        """
        _testPresetSeeder_

        Test whether the PresetSeeder works
        """

        task1 = makeWMTask("task2")

        seederDict = {"generator.initialSeed": 1001, "evtgenproducer.initialSeed": 1001}
        task1.addGenerator("PresetSeeder", **seederDict)

        manager = GeneratorManager(task = task1)


        jobs = self.oneHundredFiles()

        for jobGrp in jobs:
            manager(jobGrp)

        for jobGrp in jobs:
            count = 0
            for job in jobGrp.jobs:
                conf = job.getBaggage()
                self.assertEqual(conf.PresetSeeder.evtgenproducer.initialSeed, 1001)
                self.assertEqual(conf.PresetSeeder.generator.initialSeed,      1001)


        return



if __name__ == '__main__':
    unittest.main()
