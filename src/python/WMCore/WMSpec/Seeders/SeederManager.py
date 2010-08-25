#!/usr/bin/env python
"""
_SeederManager_

Util to instantiate a set of feeders based on settings from a WMStep
and then apply them to job groups

"""

import inspect

from WMCore.WMSpec.Seeders.SeederFactory import getSeeder

from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper

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
        for seederName in task.data.seeders.listSections_():
            confValues = getattr(task.data.seeders, seederName)
            args = {}
            for entry in confValues.listSections_():
                value = getattr(confValues, entry)
                args[entry] = value
            self.addSeeder(seederName, **args)


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

