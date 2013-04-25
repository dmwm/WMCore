#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_LHEStepZero_
Created on Mon May  7 22:29:00 2012

LHE Step0 workflow

@author: dballest
"""

from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.WMSpec.StdSpecs.MonteCarlo import getTestArguments as testBaseArguments

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required
    by the standard LHE Step0 workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    args = testBaseArguments()
    args['EventsPerLumi'] = 50

    return args

class LHEStepZeroWorkloadFactory(MonteCarloWorkloadFactory):
    """
    _LHEStepZeroWorkloadFactory_

    Generate LHE Step0 workflows

    """

    def __init__(self):
        MonteCarloWorkloadFactory.__init__(self)
        self.lheInputFiles = False

    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for a LHE Step0 request
        Just pass it down to MonteCarlo
        """
        #Override splitting arguments
        # Splitting arguments
        timePerEvent     = int(arguments.get('TimePerEvent', 60))
        filterEfficiency = float(arguments.get('FilterEfficiency', 1.0))
        totalTime        = int(arguments.get('TotalTime', 9 * 3600))
        self.totalEvents = int(int(arguments['RequestNumEvents']) / filterEfficiency)
        if arguments.get("LheInputFiles", False) == True \
             or arguments.get("LheInputFiles", False) == "True":
            self.lheInputFiles = True

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        eventsPerJob = int(totalTime/timePerEvent/filterEfficiency)
        self.prodJobSplitAlgo  = arguments.setdefault("ProdJobSplitAlgo", 'EventBased')
        self.prodJobSplitArgs  = arguments.setdefault("ProdJobSplitArgs",
                                               {"events_per_job": eventsPerJob,
                                                "events_per_lumi": arguments['EventsPerLumi']})
        self.prodJobSplitArgs.setdefault("lheInputFiles", self.lheInputFiles)
        mcWorkload = MonteCarloWorkloadFactory.__call__(self, workloadName, arguments)
        mcWorkload.setBlockCloseSettings(mcWorkload.getBlockCloseMaxWaitTime(), 5,
                                         250000000, mcWorkload.getBlockCloseMaxSize())

        return mcWorkload

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        MonteCarloWorkloadFactory.validateSchema(self, schema)
        #Now let's validate new stuff
        self.validateEventsPerLumi(schema)

    def validateEventsPerLumi(self, schema):
        """
        _validateEventsPerLumi_

        Make sure the value of the events per lumi input makes sense
        """
        eventsPerLumi = schema.get('EventsPerLumi',None)
        if not eventsPerLumi:
            self.raiseValidationException(msg = 'No events per lumi information was entered')
        try:
            eventsPerLumi = int(eventsPerLumi)
        except ValueError:
            self.raiseValidationException(msg = 'The events per lumi input from the user is invalid, only positive numbers allowed')
        if not eventsPerLumi > 0:
            self.raiseValidationException(msg = 'The events per lumi input from the user is invalid, only positive numbers allowed')
        if eventsPerLumi > int(schema['RequestNumEvents']):
            self.raiseValidationException(msg = 'More events per lumi than total events requested')

def lheStepZeroWorkload(workloadName, arguments):
    """
    _lheStepZeroWorkload_

    Instantiate the LHEStepZeroWorkloadFactory and have it generate a workload for
    the given parameters.

    """
    myLHEStepZeroFactory = LHEStepZeroWorkloadFactory()
    return myLHEStepZeroFactory(workloadName, arguments)
