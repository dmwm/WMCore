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
        self.options  = options
        self._inst    = random.SystemRandom()
        self._MAXINT  = 900000000
        #Allow the user to set the integer range
        if 'MAXINT' in options.keys():
            self._MAXINT = int(options['MAXINT'])
            del self.options['MAXINT']

    def __call__(self, wmbsJob):
        baggage = wmbsJob.getBaggage()
        for x in self.options.keys():
            wmbsJob.addBaggageParameter("RandomSeeder.%s" %(x), self._inst.randint(1, self._MAXINT))






