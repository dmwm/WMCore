#!/usr/bin/env python
"""
_TaskMaker_

This should take an incoming WMWorkload, break it into tasks
and then for each task do the jobs necessary for the task
to start as a proper job.

"""
from __future__ import print_function

import logging
import os.path
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMRuntime.SandboxCreator import SandboxCreator
from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper


# The class should carry the following variables:
#  self.workload = WMSpec.WMWorkloadHelper object, loaded by loadWorkload
#  self.workdir  = string carrying the name of an open working directory, loaded by loadWorkdir
#  self.workflowDict = dictionary of WMBS.Workflows keyed by task name;
#  self.subDict  = dictionary of WMBS.Subscriptions keyed by task name;
#


class TaskMaker:
    """
    Class for separating and starting all tasks in a WMWorkload

    """

    def __init__(self, workload=None, workdir=None):
        self.workload = None
        self.workdir = None
        self.skipSubscription = False
        self.workflowDict = {}
        self.subDict = {}
        self.owner = "unknown"
        self.owner_dn = "unknown"

        myThread = threading.currentThread()
        myThread.logger = logging.getLogger()

        self.loadWorkload(workload)
        self.loadWorkdir(workdir)

        return

    def loadWorkload(self, inputWorkload):
        """
        If workload is sane, then use it

        """

        if inputWorkload is None:
            self.workload = None
        if isinstance(inputWorkload, WMWorkload):
            self.workload = WMWorkloadHelper(inputWorkload)
            return
        if isinstance(inputWorkload, WMWorkloadHelper):
            self.workload = inputWorkload
            return

        if not os.path.exists(inputWorkload):
            raise Exception('Could not find %s in local file system' % (str(inputWorkload)))

        testWorkload = WMWorkloadHelper(WMWorkload("workload"))
        testWorkload.load(inputWorkload)

        self.workload = testWorkload

        return

    def loadWorkdir(self, inputWorkdir):
        """
        If workdir is sane, use it

        """

        if inputWorkdir is None:
            self.workdir = None
            return

        if not os.path.isdir(inputWorkdir):
            try:
                os.mkdir(inputWorkdir)
            except Exception as ex:
                print('Caught exception %s in creating workdir %s' % (str(ex), inputWorkdir))
                raise Exception(ex)

        self.workdir = inputWorkdir

        return

    def processWorkload(self):
        """
        Split the workload into tasks, and finish all the process for the task

        """

        if self.workdir is None or self.workload is None:
            raise Exception(
                'Failure in processing workload %s in directory %s' % (str(self.workload), str(self.workdir)))

        if hasattr(self.workload, 'owner'):
            self.owner = self.workload.owner
        if hasattr(self.workload, 'owner_dn'):
            self.owner_dn = self.workload.owner_dn

        for toptask in self.workload.taskIterator():
            # for each task, build sandbox, register, and subscribe
            for task in toptask.taskIterator():
                if task.name() in self.workflowDict.keys():
                    raise Exception(
                        'Duplicate task name for workload %s, task %s' % (self.workload.name(), task.name()))

                if not self.skipSubscription:
                    self.subscribeWMBS(task)

        sandboxCreator = SandboxCreator()
        sandboxCreator.makeSandbox(self.workdir, self.workload)
        logging.info('Done processing workload %s' % (self.workload.name()))

        return True

    def createWorkflow(self, task):
        """
        Register job into WMBS for each task through Workflows

        """

        specURL = self.getWorkflowURL(task)

        fileSet = Fileset(name=self.getFilesetName(task), is_open=True)
        fileSet.create()

        taskFlow = Workflow(spec=specURL, owner=self.owner, dn=self.owner_dn,
                            name=self.getWorkflowName(task), task=task.name())
        taskFlow.create()

        self.workflowDict[task.name()] = taskFlow

        # Insert workflow into task
        setattr(task.data.input.WMBS, 'WorkflowSpecURL', specURL)

        # If the job is a merge job
        # Find the task it merges from
        # Then find the workflow for that task and assign it an output
        if hasattr(task.inputReference(), 'outputModule'):
            dummyStepName = task.inputReference().inputStep.split('/')[-1]
            taskName = task.inputReference().inputStep.split('/')[-2]
            outputModule = task.inputReference().outputModule
            if taskName not in self.workflowDict.keys():
                raise Exception('I am being asked to chain output for a task %s which does not yet exist' % taskName)
            outputWorkflow = self.workflowDict[taskName]
            outputWorkflow.addOutput(outputModule, fileSet)

        logging.info('Registered workflow for step %s' % (task.name()))

        return taskFlow, fileSet

    def subscribeWMBS(self, task):
        """
        Create a subscription for each task

        """

        workFlow, fileSet = self.createWorkflow(task)

        workFlow.load()
        fileSet.load()

        subType = ''
        if task.name() == 'Processing' or task.name() == 'Production':
            subType = 'Processing'
        elif task.name() == 'Merge':
            subType = 'Merge'

        newSub = Subscription(fileset=fileSet, workflow=workFlow, split_algo='FileBased', type=subType)
        newSub.create()

        # Add subscription to dictionary
        self.subDict[task.name()] = newSub

        # Add subscription id to task
        setattr(task.data.input.WMBS, 'Subscription', newSub['id'])

        if not newSub.exists() >= 0:
            raise Exception("ERROR: Subscription does not exist after it was created")

        logging.info('Created subscription for task %s' % (task.name()))

        return

    def getFilesetName(self, task):
        """
        Create a unique name for a fileset

        """

        filesetName = '%s/Fileset-%s-%s' % (self.workdir, self.workload.name(), task.name())

        return filesetName

    def getWorkflowURL(self, task):
        """
        Create a unique name and location for each workflow

        """

        workflowURL = '%s/%s' % (self.workdir, self.getWorkflowName(task))

        return workflowURL

    def getWorkflowName(self, task):
        """
        Create a unique name for each workflow

        """

        workflowName = 'Workflow-%s-%s' % (self.workload.name(), task.name())

        return workflowName
