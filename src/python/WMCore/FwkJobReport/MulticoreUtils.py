#!/usr/bin/env python
# encoding: utf-8
"""
MulticoreUtils.py

Created by Dave Evans on 2011-07-05.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
from WMCore.Configuration import ConfigSection

#
# util lambdas
#
average = lambda x : sum(x)/len(x)
getSectParam = lambda x, sect, param: getattr( getattr(x, sect), param)

#
# Definition of what fields will be combined using what method of combination
#
AggrFunctions = {
    "memory.PeakValueRss" : sum ,
    "memory.PeakValueVsize" : sum ,
    "memory.PeakValuePss": sum,
    "storage.writeTotalMB" : sum,
    "storage.readPercentageOps": average,
    "storage.readAveragekB": average,
    "storage.readTotalMB": sum,
    "storage.readNumOps": sum ,
    "storage.readCachePercentageOps": average,
    "storage.readMBSec": average,
    "storage.readMaxMSec": average,
    "storage.readTotalSecs": sum ,
    "storage.writeTotalSecs": sum,
    "cpu.TotalJobCPU": sum,
    "cpu.TotalEventCPU": sum,
    "cpu.AvgEventCPU": average,
    "cpu.AvgEventTime": average,
    "cpu.MinEventCPU": min ,
    "cpu.MaxEventTime": max,
    "cpu.TotalJobTime": average,
    "cpu.MinEventTime": min,
    "cpu.MaxEventCPU": max,
    }





class Aggregator(object):
    """
    _Aggregator_

    Util to aggregate performance reports for multicore jobs into a single
    performance report, including a multicore section to allow profiling of internal performance

    """
    def __init__(self):
        self.numCores = 0
        self.sections = {}
        self.values = {}
        self.report = ConfigSection("performance")
        #
        # populate the aggregator with the list of expected keys
        # based on the functions map above
        # create a combined performance report with the appropriate sections
        for red in AggrFunctions.keys():
            self.values[red] = []
            sect, param = red.split(".")
            if not self.sections.has_key(sect):
                self.sections[sect] = []
                self.report.section_(sect)
            if param not in self.sections[sect]:
                self.sections[sect].append(param)




    def add(self, perfRep):
        """
        _add_

        Add the contents of the given performance rep to this
        aggregator
        """
        self.numCores += 1
        for sect in self.sections.keys():
            for param in self.sections[sect]:
                key = ".".join([sect, param])
                try:
                    #protect against weird cases like NaNs in the reports
                    value = float(getSectParam(perfRep, sect, param))
                except AttributeError:
                    #protect against missing parameters
                    continue
                except ValueError:
                    continue
                self.values[key].append(value)


    def aggregate(self):
        """
        _aggregate_

        For each key in the map, run the appropriate aggregation function on it
        """
        for key, vals in self.values.items():
            # avoid divide by zero averages etc
            if len(vals) == 0: continue
            aggFunc = AggrFunctions[key]
            sect, param = key.split(".")
            section = getattr(self.report, sect)
            setattr(section, param, aggFunc(vals))
        self.createMulticoreSection()
        return self.report



    def createMulticoreSection(self):
        """
        _createMulticoreSection_

        create the multicore report section
        """
        self.report.section_("multicore")
        self.report.multicore.coresUsed = self.numCores
        if self.values.has_key("cpu.TotalJobTime"):
            vals = self.values["cpu.TotalJobTime"]
            self.report.multicore.summedProcessTime = sum(vals)
            self.report.multicore.averageProcessTime = average(vals)
            self.report.multicore.maxProcessTime = max(vals)
            self.report.multicore.minProcessTime = min(vals)
            self.report.multicore.processWaitingTime = max(vals) - min(vals)

            stepEffNom = float(sum(vals)) / float((max(vals) * self.numCores))
            stepEffDenom = float(average(vals))   / float(max(vals))
            stepEff = stepEffNom/stepEffDenom
            self.report.multicore.stepEfficiency = stepEff


        # frame in the merge report values
        # need to be set from the MulticoreCMSSW Executor
        self.report.multicore.mergeStartTime = None
        self.report.multicore.mergeEndTime = None
        self.report.multicore.numberOfMerges = None
        self.report.multicore.totalStepTime = None
        self.report.multicore.averageMergeTime = None
        self.report.multicore.maxMergeTime = None
        self.report.multicore.minMergeTime = None




def updateMulticoreReport(reportInstance, numMerges, mergeStart, mergeEnd, totalStepTime, *mergeData):
    """
    _updateMulticoreReport_

    Function to add in multicore performance numbers
    Args should be pretty self explanatory, the mergeData is the list of times taken for each merge job

    """
    sect = reportInstance.report.performance.multicore
    sect.mergeStartTime = mergeStart
    sect.mergeEndTime = mergeEnd
    sect.numberOfMerges = numMerges
    sect.totalStepTime = totalStepTime
    if len(mergeData) == 0: return
    sect.maxMergeTime = max(mergeData)
    sect.minMergeTime = min(mergeData)
    sect.averageMergeTime = average(mergeData)
