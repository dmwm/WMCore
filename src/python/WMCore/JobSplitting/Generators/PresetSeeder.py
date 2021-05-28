#!/usr/bin/env python
"""
_RandomSeeder_

Simple seed generator for preset seeds

"""

from WMCore.JobSplitting.Generators.GeneratorInterface import GeneratorInterface

class PresetSeeder(GeneratorInterface):
    """
    Seeder that inserts for each initial seed the value
    sent by the dictionary passed through.

    """
    def __init__(self, **options):
        self.options = options


    def __call__(self, wmbsJob):
        baggage = wmbsJob.getBaggage()
        for x in self.options:
            wmbsJob.addBaggageParameter("PresetSeeder.%s" %(x), self.options[x])
