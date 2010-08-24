#!/usr/bin/env python
"""
_SeederManager_

Util to instantiate a set of feeders based on settings from a WMStep
and then apply them to job groups

"""

from WMCore.WMSpec.Seeders.SeederFactory import getSeeder

from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper
from WMCore.WMSpec.ConfigSectionTree import *

class SeederManager:
    """
    _SeederManager_



    """
    def __init__(self, task = None):
        self.seeders = {}

        if not hasattr(task, 'data'):
            #We don't have a WMTask
            return
        if not hasattr(task.data, 'seeders'):
            #We have a blank task with no seeders
            return
        #Otherwise we have a fully formed task of some type

        configList = task.listGenerators()

        for seederName in configList:
            self.addSeeder(seederName, task.getGeneratorSettings(seederName))


        return

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


    def getSeederList(self):
        """
        _getSeederList_

        Returns a list of all seeders for usage in JobSplitting
        """
        seederList = []

        for name in self.seeders.keys():
            seederList.append(self.seeders[name])

        return seederList


