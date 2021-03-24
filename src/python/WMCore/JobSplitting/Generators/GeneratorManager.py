#!/usr/bin/env python
"""
_SeederManager_

Util to instantiate a set of feeders based on settings from a WMStep
and then apply them to job groups

"""

from builtins import map, object
from future.utils import viewvalues

from WMCore.JobSplitting.Generators.GeneratorFactory import GeneratorFactory

from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper
from WMCore.WMSpec.ConfigSectionTree import *

class GeneratorManager(object):
    """
    _GeneratorManager_



    """
    def __init__(self, task = None):
        self.generators = {}
        self.generatorFactory = GeneratorFactory()

        if not hasattr(task, 'data'):
            #We don't have a WMTask
            return
        if not hasattr(task.data, 'generators'):
            #We have a blank task with no generators
            return
        #Otherwise we have a fully formed task of some type

        configList = task.listGenerators()

        for generator in configList:
            self.addGenerator(generator, **task.getGeneratorSettings(generator))


        return

    def addGenerator(self, generatorName, **args):
        """
        _addGenerator_

        Add a new instance of the generator provided

        """
        if generatorName == None:
            return
        if generatorName in self.generators:
            return

        #TODO: Exception check
        newGenerator = self.generatorFactory.getGenerator(generatorName, **args)

        self.generators[generatorName] = newGenerator
        return



    def __call__(self, jobGroup):
        """
        _operator(jobGroup)_

        Run all generators in this manager over the jobs in the
        job group provided

        """
        [list(map(generator, jobGroup.jobs)) for generator in viewvalues(self.generators)]
        return


    def getGeneratorList(self):
        """
        _getGeneratorList_

        Returns a list of all generators for usage in JobSplitting
        """
        generatorList = []

        for generator in viewvalues(self.generators):
            generatorList.append(generator)

        return generatorList
