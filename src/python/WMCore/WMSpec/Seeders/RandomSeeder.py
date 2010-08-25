#!/usr/bin/env python
"""
_RandomSeeder_

Simple Random seed generator

"""
import random

from WMCore.WMSpec.Seeders.SeederInterface import SeederInterface

class RandomSeeder(SeederInterface):
    """
    Seeder that generates a set of random numbers for a job
    and inserts them into the job baggage

    """
    def __init__(self, **options):
        self.seedlist = options.get("seed_list", [])
        self._inst = random.SystemRandom()
        self._MAXINT = 900000000


    def __call__(self, wmbsJob):
        baggage = wmbsJob.getBaggage()
        [ wmbsJob.addBaggageParameter(
            x, self._inst.randint(1, self._MAXINT))
          for x in self.seedlist ]




