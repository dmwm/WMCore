#!/usr/bin/env python
"""
_RandomSeeder_

Simple Random seed generator

"""
import random

from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface

class RandomSeeder(GeneratorInterface):
    """
    Seeder that generates a set of random numbers for a job
    and inserts them into the job baggage

    """
    def __init__(self, **options):
        self.options  = options
        self._MAXINT  = options.get('MAXINT', 900000000)

    def __call__(self, wmbsJob):
        baggage = wmbsJob.getBaggage()
        for x in self.options:
            wmbsJob.addBaggageParameter("RandomSeeder.%s" %(x), random.randint(1, self._MAXINT))
