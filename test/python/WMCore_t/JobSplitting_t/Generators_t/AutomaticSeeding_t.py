#!/usr/bin/env python
# encoding: utf-8
"""
AutomaticSeeding_t.py

Created by Dave Evans on 2010-08-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from __future__ import print_function

import sys
import os
import unittest


from WMCore.JobSplitting.Generators.AutomaticSeeding import AutomaticSeeding
from WMCore.DataStructs.Job import Job
from PSetTweaks.PSetTweak import PSetTweak

class AutomaticSeeding_tTests(unittest.TestCase):


    def testA(self):
        """test creating the plugin"""
        try:
            seeder = AutomaticSeeding()
        except Exception as ex:
            msg = "Failed to instantiate an AutomaticSeeder: "
            msg += str(ex)
            self.fail(msg)



    def testB(self):
        """test plugin acts on a Job as expected"""

        job = Job("TestJob")
        seeder = AutomaticSeeding()
        seeder(job)

    def testC(self):
        """test building a tweak from the seeds"""
        job = Job("TestJob")
        seeder = AutomaticSeeding()

        job.addBaggageParameter("process.RandomNumberGeneratorService.seed1.initialSeed", 123445)
        job.addBaggageParameter("process.RandomNumberGeneratorService.seed2.initialSeed", 123445)
        job.addBaggageParameter("process.RandomNumberGeneratorService.seed3.initialSeed", 7464738)
        job.addBaggageParameter("process.RandomNumberGeneratorService.seed44.initialSeed", 98273762)


        seeder(job)

        tweak = PSetTweak()
        for x in job.baggage.process.RandomNumberGeneratorService:
            parameter = "process.RandomNumberGeneratorService.%s.initialSeed" % x._internal_name
            tweak.addParameter(parameter, x.initialSeed)
        print(tweak)




if __name__ == '__main__':
    unittest.main()
