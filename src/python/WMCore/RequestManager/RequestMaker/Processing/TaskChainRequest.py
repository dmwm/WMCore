#!/usr/bin/env python
# encoding: utf-8
"""
TaskChainRequest.py

Created by Dave Evans on 2011-07-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.TaskChain import taskChainWorkload


class TaskChainRequest(RequestMakerInterface):
    """
    _TaskChainRequest_

    RequestMaker for a TaskChain request

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        return taskChainWorkload(schema['RequestName'], schema).data


def validateSubTask(task, firstTask = False):
    """
    _validateSubTask_

    Check required fields for a sub task
    """
    reqKeys = ["TaskName", "SplittingAlgorithm", "SplittingArguments"]
    for rK in reqKeys:
        if not task.has_key(rK):
            msg = "Sub Task missing Required Key: %s\n" % rK
            msg += str(task)
            raise RuntimeError(msg)
    #
    # input definition checks
    #
    if not firstTask:
        if not task.has_key("InputTask"):
            msg = "Task %s has no InputTask setting" % task['TaskName']
            raise RuntimeError(msg)
        if not task.has_key("InputFromOutputModule"):
            msg = "Task %s has no InputFromOutputModule setting" % task['TaskName']
            raise RuntimeError(msg)

    # configuration checks
    check = task.has_key("Scenario") or task.has_key("ConfigCacheID")
    if not check:
        msg = "Task %s has no Scenario or ConfigCacheID, one of these must be provided" % task['TaskName']
        raise RuntimeError(msg)
    if task.has_key("Scenario"):
        if not task.has_key("ScenarioMethod"):
            msg = "Scenario Specified for Task %s but no ScenarioMethod provided" % task['TaskName']
            raise RuntimeError(msg)
        scenArgs = task.get("ScenarioArgs", {})
        if not scenArgs.has_key("writeTiers"):
            msg = "task %s ScenarioArgs does not contain writeTiers argument" % task['TaskName']
            raise RuntimeError, msg





def validateGenFirstTask(task):
    """
    _validateGenFirstTask_

    Validate first task contains all stuff required for generation
    """
    if not task.has_key("RequestSizeEvents"):
        msg = "RequestSizeEvents is required for first Generator task"
        raise RuntimeError(msg)

    if not task.has_key("PrimaryDataset"):
        msg = "PrimaryDataset is required for first Generator task"
        raise RuntimeError(msg)

def validateProcFirstTask(task):
    """
    _validateProcFirstTask_

    Validate that Processing First task contains required params
    """
    if task['InputDataset'].count('/') != 3:
        raise RuntimeError("Need three slashes in InputDataset %s " % task['InputDataset'])


class TaskChainSchema(RequestSchema):
    """
    _TaskChainSchema_

    Spec out required data for TaskChain style requests.
    See WMSpec/StdSpecs/TaskChain.py for details of the spec & nested dictionary
    structure.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion",
            "AcquisitionEra",
            "ScramArch",
            "GlobalTag",
            "TaskChain",
            ]
        self.optionalFields = [
            "SiteWhitelist",
            "SiteBlacklist",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "ProcessingVersion",
            "CouchUrl",
            "CouchDBName",
            "DbsUrl",
            "UnmergedLFNBase",
            "MergedLFNBase",
            "MinMergeSize",
            "MaxMergeSize",
            "MaxMergeEvents"
            ]

    def validate(self):
        """
        _validate_

        Check that basic required parameters are provided and do some more
        detailed checks on each of the parameters for the chain sub steps
        """
        RequestSchema.validate(self)

        try:
            numTasks = int(self['TaskChain'])
        except ValueError:
            msg = "TaskChain parameter is not an Integer"
            raise RuntimeError(msg)

        for i in range (1, numTasks+1):
            if not self.has_key("Task%s" % i):
                msg = "No Task%s entry present in request" % i
                raise RuntimeError, msg

            if i == 1:
                if self['Task1'].has_key('InputDataset'):
                    validateProcFirstTask(self['Task1'])
                else:
                    validateGenFirstTask(self['Task1'])
                validateSubTask(self['Task1'], firstTask = True)
            else:
                validateSubTask(self['Task%s' % i])



registerRequestType("TaskChain", TaskChainRequest, TaskChainSchema)
