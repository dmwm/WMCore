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
        self.seedlist = options.keys()
        self._inst    = random.SystemRandom()
        self._MAXINT  = 900000000
        #Allow the user to set the integer range
        if 'MAXINT' in options.keys():
            self._MAXINT = options['MAXINT']
            self.seedlist.remove('MAXINT')

    def __call__(self, wmbsJob):
        baggage = wmbsJob.getBaggage()
        for x in self.seedlist:
            wmbsJob.addBaggageParameter("RandomSeeder.%s.initialSeed" %(x), self._inst.randint(1, self._MAXINT))






