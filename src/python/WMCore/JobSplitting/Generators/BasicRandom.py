#!/usr/bin/env python
"""
_RandomSeeder_

Simple Random seed generator

"""
import random
from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface



class BasicRandom(GeneratorInterface):
    """
    Generator that generates a set of random numbers for a job
    and inserts them into the job baggage

    """
    def __init__(self, **options):
        GeneratorInterface.__init__(self, **options)
        self.instance = random.SystemRandom()
        self.maxInt   = options.get("maxint", 900000000)
        self.seeds    = options.get("seed_list", [])


    def __call__(self, wmbsJob):
        baggage = wmbsJob.getBaggage()
        for x in self.options['seed_list']:
            wmbsJob.addBaggageParameter(
                "RandomSeeder.%s" %(x),
                self.instance.randint(1, self.maxInt)
                )
