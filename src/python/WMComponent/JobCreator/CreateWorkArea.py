#!/usr/bin/env python
# pylint: disable=E1101, E1103, C0121
# E1101 doesn't allow you to define config sections using .section_()
# E1103: Transaction attached to myThread


"""
_CreateWorkArea_

Class(es) that create the work area for each jobGroup
Used in JobCreator
"""
from __future__ import division

from builtins import object

import logging
import os
import os.path
import threading
import traceback
from subprocess import Popen, PIPE

from Utils.IteratorTools import grouper
from WMCore.DAOFactory import DAOFactory
from WMCore.WMException import WMException
from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper


def createDirectories(dirList):
    """
    Create the directory if everything is sane

    """
    for sdirList in grouper(dirList, 500):
        cmdArgs = ['mkdir']
        cmdArgs.extend(sdirList)
        pipe = Popen(cmdArgs, stdout=PIPE, stderr=PIPE, shell=False)
        stdout, stderr = pipe.communicate()
        if stderr:
            if hasattr(stderr, "decode"):
                stderr = stderr.decode('utf-8', 'ignore')
            msg = "Error in creating directories: %s\n" % stderr
            logging.error(msg)
            raise CreateWorkAreaException(msg)

    return


def makedirs(directory):
    """
    _makedirs_

    Implementation of os.makedirs
    You can run subscriptions from the same workflow in separate instances
    of the JobCreatorWorker.  This exists to make sure that if they collide,
    a rare but observed possibility, they don't kill everything.
    """

    try:
        os.makedirs(directory)
    except:
        if not os.path.isdir(directory):
            # Then it really screwed up
            msg = "Failed to create directory %s\n" % (directory)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise CreateWorkAreaException(msg)
        # Else: the directory exists.  Don't complain, but do mention it
        else:
            msg = "Hit error in creating directory %s; ignoring. \n" % (msg)
            msg += str(traceback.format_exc())
            msg = "This looks like an error but everything seems to be in place"
            logging.error(msg)

    return


def getMasterName(startDir, wmWorkload=None, workflow=None):
    """
    Gets a universal name for the jobGroup directory
    Return the uid as the name if none available (THIS SHOULD NEVER HAPPEN)

    """

    if wmWorkload != None:
        workload = wmWorkload.name()
    elif not os.path.exists(workflow.spec):
        msg = "Could not find Workflow spec %s: " % (workflow.spec)
        msg += "Cannot create work area without spec!"
        logging.error(msg)
        raise CreateWorkAreaException(msg)
    else:
        wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
        wmWorkload.load(workflow.spec)

        workload = wmWorkload.name()

    task = workflow.task
    if task.startswith("/" + workload + "/"):
        task = task[len(workload) + 2:]

    return (os.path.join(startDir, workload),
            os.path.join(startDir, workload, task))


class CreateWorkAreaException(WMException):
    """
    This is a totally awesome exception class

    Except, y'know, that it's not.
    """

    pass


class CreateWorkArea(object):
    """
    Basic class for doing the JobMaker dirty work

    """

    def __init__(self, jobGroupID=None, startDir=None):

        myThread = threading.currentThread()
        myThread.logger = logging.getLogger()

        self.jobGroupID = jobGroupID
        self.jobGroup = None
        self.subscript = None
        self.workflow = None
        self.collectionDir = None
        self.wmWorkload = None
        if not startDir:
            self.startDir = os.getcwd()
        else:
            self.startDir = startDir

        self.jobs = {}

        self.getNewJobGroup(jobGroupID)

        return

    def reset(self):
        """
        Reset key variables between runs

        """

        self.workflow = None
        self.wmWorkload = None

    def processJobs(self, jobGroup, startDir=None, wmWorkload=None,
                    workflow=None, transaction=None, conn=None, cache=True):
        """
        Process the work

        This allows you to pass in two pre-loaded objects, the WMWorkloadSpace and the
        WMBS workflow, to save loading time
        """
        self.reset()
        self.wmWorkload = wmWorkload
        self.workflow = workflow
        self.startDir = startDir
        self.transaction = transaction
        self.conn = conn

        self.jobGroup = jobGroup

        # self.getNewJobGroup(jobGroup = jobGroup)
        self.createJobGroupArea()
        self.createWorkArea(cache=cache)

        return

    def getNewJobGroup(self, jobGroup=None):
        """
        This gets a job group passed to the thread
        """

        # See if we actually have a jobGroupID
        if jobGroup:
            self.jobGroupID = jobGroup.id
        else:
            # Then we have no jobGroup
            return


            # We need the workflow to get the spec
            # if self.workflow == None:
            # If we have something in the workflow,
            # assume we were passed a loaded workflow
            # We need the subscription mostly to get the workflow
            # subscript = jobGroup.subscription
            # subscript.load()
            # self.workflow  = subscript['workflow']
            # self.workflow.load()

        # if not jobGroup.exists():
        #    msg = 'JobMaker: Was passed a non-existant Job Group ID %i' % (self.jobGroupID)
        #    logging.error(msg)
        #    raise Exception(msg)

        self.jobGroup = jobGroup

        return

    def createJobGroupArea(self):
        """
        Creates an area for the task which is just the jobGroupUID,
        in which the jobs will be put

        """

        workloadDir, taskDir = getMasterName(startDir=self.startDir,
                                             wmWorkload=self.wmWorkload,
                                             workflow=self.workflow)

        # Create the workload directory
        if not os.path.isdir(workloadDir):
            makedirs(directory=workloadDir)

        # Create the task directory
        if not os.path.isdir(taskDir):
            makedirs(directory=taskDir)

        return

    def createWorkArea(self, cache=True):
        """
        This should handle the master tasks of creating a working area
        It should take a valid jobGroup and call the
        functions that create the components

        """
        myThread = threading.currentThread()

        if self.jobGroup == None:
            msg = 'JobMaker: Ordered to create for non-existant jobGroup  Failing'
            logging.error(msg)
            raise CreateWorkAreaException(msg)

        workloadDir, taskDir = getMasterName(startDir=self.startDir,
                                             wmWorkload=self.wmWorkload,
                                             workflow=self.workflow)
        jobCounter = 0
        nameList = []

        if cache:
            factory = DAOFactory("WMCore.WMBS", myThread.logger, myThread.dbi)
            setBulkCache = factory(classname="Jobs.SetCache")
        nameDictList = []

        # Now actually start to do things
        for job in self.jobGroup.jobs:
            jid = job['id']

            if jobCounter % 1000 == 0:
                # Create a new jobCollection
                # Increment jobCreator if there's already something there
                jobCounter += self.createJobCollection(jobCounter, taskDir)

            jobCounter = jobCounter + 1

            name = self.getDirectoryName(jid)
            nameList.append(name)
            nameDictList.append({'jobid': jid, 'cacheDir': name})
            job['cache_dir'] = name

        if cache:
            setBulkCache.execute(jobDictList=nameDictList,
                                 conn=self.conn,
                                 transaction=self.transaction)

        createDirectories(nameList)

        # change permissions. See #3623
        for directory in nameList:
            os.chmod(directory, 0o775)

        return

    def createJobCollection(self, jobCounter, taskDir):
        """

        Create a sub-directory to allow storage of large jobs
        """

        value = int(jobCounter // 1000)
        jobCollDir = '%s/JobCollection_%i_%i' % (taskDir, self.jobGroup.id, value)
        # Set this to a global variable
        self.collectionDir = jobCollDir
        if not os.path.exists(jobCollDir):
            # This should be the only application
            # You return 0 because the directory you just made should be empty
            os.mkdir(jobCollDir)
            return 0
        if os.path.isdir(jobCollDir):
            # This should never happen
            return len(os.listdir(jobCollDir))
        elif os.path.isfile(jobCollDir):
            # Well, you're screwed.  Some other file is in the way: IN A DIRECTORY YOU JUST CREATED.
            # Time to freak the hell out
            msg = "Could not create jobCollection %s;" % (jobCollDir)
            msg += "non-directory file in the way!"
            logging.error(msg)
            raise CreateWorkAreaException(msg)
        else:
            # You're screwed
            msg = "Something is in the way at %s" % (jobCollDir)
            msg += "File system could not determine type of object"
            logging.error(msg)
            raise CreateWorkAreaException(msg)

    def getDirectoryName(self, jid):
        """
        Gets a universal name for the directory we're working in.

        """

        name = 'job_%i' % (jid)

        return os.path.join(self.collectionDir, name)
