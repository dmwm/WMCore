#!/usr/bin/env python
"""
_SeederManager_

Util to instantiate a set of feeders based on settings from a WMStep
and then apply them to job groups

"""


from WMCore.WMSpec.Seeders.SeederFactory import getSeeder



class SeederManager:
    """
    _SeederManager_



    """
    def __init__(self):
        self.seeders = {}


    def addSeeder(self, seederName, **args):
        """
        _addSeeder_

        Add a new instance of the seeder provided

        """
        if self.seeders.has_key(seederName): return

        #TODO: Exception check
        newSeeder = getSeeder(seederName, **args)

        self.seeders[seederName] = newSeeder
        return



    def __call__(self, jobGroup):
        """
        _operator(jobGroup)_

        Run all seeders in this manager over the jobs in the
        job group provided

        """
        [ map(seeder, jobGroup.jobs) for seeder in self.seeders.values()]
        return

