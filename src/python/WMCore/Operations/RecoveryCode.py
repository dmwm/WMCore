#!/bin/env python

"""
_recoveryCode_

This is the code that handles the complete disintegration of the database.
This code should never be run without the permission of a dataOps manager or
the local authority, and should never be run except following a crash of the
system because it will wipe out all running and queued jobs.
"""


import os
import os.path
import logging
import shutil
import time

from WMCore.WMBS.Job import Job

from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPoller
from WMComponent.JobTracker.JobTrackerPoller   import JobTrackerPoller


def findAllJobs(startDir = None):
    """
    Find all jobs with cache on disk

    """

    currentTime = time.time()

    if not os.path.isdir(startDir):
        msg = "Cannot locate jobCacheDir!  Cannot reconstruct jobs!"
        logging.error(msg)
        raise Exception(msg)

    jobList = []

    #Assume that we're totally hosed, but that all Created and later jobs are on disk
    #Hunt through disk for jobs

    for workflowDir in os.listdir(startDir):
        for taskDir in os.listdir('%s/%s' % (startDir, workflowDir)):
            #Now we are in the individual tasks, which should be filled with jobCollections
            tDir = '%s/%s/%s' % (startDir, workflowDir, taskDir)
            for jobCollection in os.listdir(tDir):
                for jobDir in os.listdir('%s/%s' % (tDir, jobCollection)):
                    tmpJob = Job(id = int(jobDir.split('_')[1]))
                    tmpJob['name'] = 'RecoveryJobAttempt_%i_%f' % (tmpJob['id'], currentTime)
                    tmpJob['cache_dir'] = '%s/%s/%s' % (tDir, jobCollection, jobDir)
                    jobList.append(tmpJob)

    return jobList


def killJobs(list, config):
    """
    Kill all the jobs in the batch system whose ID comes off of the list

    """

    jobList = []

    for job in list:
        jobList.append(job['id'])

    jobTracker = JobTrackerPoller(config)
    jobTracker.setup(None)
    jobTracker.killJobs(jobList = jobList)

    return list



def cleanJobs(jobList, config):
    """
    Clean out all directories that were created by the job using
    the jobArchiver.

    As an input, the list should be a list of job objects, for which
    We need the names and IDs.

    """

    currentTime = time.time()

    jobArchiver = JobArchiverPoller(config)
    jobArchiver.cleanWorkArea(jobList)

    #Once you've nuked the job directories, nuke the whole workflow

    for workflowDir in os.listdir(config.JobCreator.jobCacheDir):
        for tDir in os.listdir(os.path.join(config.JobCreator.jobCacheDir, workflowDir)):
            taskDir = os.path.join(config.JobCreator.jobCacheDir, workflowDir, tDir)
            for jColl in os.listdir(taskDir):
                if jColl.find('JobCollection') > -1:
                    try:
                        shutil.rmtree(os.path.join(taskDir, jColl))
                    except ex:
                        msg = 'Error attepting to delete jobCollection: %s' %(ex)
                        raise Exception(msg)
            if os.listdir(taskDir) == []:
                #If the taskDir is empty, remove it
                shutil.rmtree(taskDir)
        if os.listdir(os.path.join(config.JobCreator.jobCacheDir, workflowDir)) == []:
            #If the workflowDir is empty, remove it
            shutil.rmtree(os.path.join(config.JobCreator.jobCacheDir, workflowDir))
                    

    return jobList


def cleanSubmitDir(jobList, config):
    """
    Clean up all the submit jdl files sent to condor or other batch system

    """

    listOfFiles = os.listdir(config.JobSubmitter.submitDir)
    for job in jobList:
        jobSubmitFile = 'submit_%i.jdl' % (job['id'])
        if jobSubmitFile in listOfFiles:
            os.remove(os.path.join(config.JobSubmitter.submitDir, jobSubmitFile))

    return jobList



class PurgeJobs:
    """
    PurgeJobs
    
    Destroy all jobs and their accompanying files and directories
    """


    def __init__(self, config):
        """
        Does nothing
        """

        self.config = config

        return

    def run(self):
        """
        Actually run everything.

        """

        jobList   = findAllJobs(startDir = self.config.JobCreator.jobCacheDir)
        killList  = killJobs(list = jobList, config = self.config)
        purgeList = cleanJobs(jobList = killList, config = self.config)
        doneList  = cleanSubmitDir(jobList = purgeList, config = self.config)


        return




    
        
