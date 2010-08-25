#!/usr/bin/env python
"""
_TaskQueueKiller_

Killer plugin for killing TaskQueue jobs

"""

__revision__ = "$Id: TaskQueueKiller.py,v 1.1 2009/12/16 18:08:05 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"

import logging
import traceback

from JobKiller.Registry import registerKiller
from JobKiller.KillerExceptions import InvalidJobException, \
                                       JobNotSubmittedException

from ProdAgent.WorkflowEntities import JobState
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentCore.Configuration import loadProdAgentConfiguration

from WMCore import Configuration as WMCoreConfig

from TQComp.Apis.TQSubmitApi import TQSubmitApi
from TQComp.Constants import taskStates

## BossLite dependencies
#from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
#from ProdCommon.BossLite.Common.Exceptions import BossLiteError, SchedulerError
#from ProdAgentDB.Config import defaultConfig as dbConfig
#from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
#from ProdCommon.BossLite.Common.System import executeCommand

class TaskQueueKiller:
    """
    _TaskQueueKiller_

    """

    def __init__(self, args):
        """

        """

        logging.debug("<<<<< Creating TaskQueueKiller>>>>>")

        # Try to get the TaskQueue conf file from ProdAgentConfig.xml
        what = "TaskQueue"

        try:
            cfg = loadProdAgentConfiguration()
            self.tqConfig = cfg.getConfig(what)
        except StandardError, ex:
            msg = "%s.Config:" % what
            msg += "Unable to load ProdAgent Config for " + what
            msg += "%s\n" % ex
            logging.critical(msg)
            raise ProdAgentException(msg)

        # Now load the TaskQueue API
        confFile = self.tqConfig['TaskQueueConfFile']
        myconfig = WMCoreConfig.loadConfigurationFile(confFile)
        self.tqApi = TQSubmitApi(logging, myconfig, None)
        logging.debug("<<<<< TaskQueueKiller created! >>>>>")


    def killJob(self, jobSpecId):
        """
        Arguments:
          JobSpecId -- the job id.

        Return:
          none

        """

#        # Verify that the job exists
#        try:
#            stateInfo = JobState.general(jobSpecId)
#        except StandardError, ex:
#            msg = "Cannot retrieve JobState Information for %s\n" % jobSpecId
#            msg += str(ex)
#            logging.error(msg)
#            raise InvalidJobException, msg

#        # Verify that it has not finished
#        if stateInfo['State'] == "finished":
#            msg = "Cannot kill job %s, since it has finished\n" % jobSpecId
#            logging.error(msg)
#            raise JobNotSubmittedException, msg


        # Kill command through TaskQueue API
        self.tqApi.killTasks([jobSpecId])

        return


    def killWorkflow(self, workflowSpecId):
        """

        Arguments:
          workflowSpecId -- the workflow id.

        Return:
          none

        """

        logging.info("TaskQueueKiller.killWorkflow(%s)" % workflowSpecId)

        # get job ids for workflows workflowSpecId
        jobs = JobState.retrieveJobIDs([workflowSpecId])
        jobs = map(lambda x: x[0], jobs)
        logging.debug("TaskQueueKiller-> jobs: %s" % str(jobs))

        totalJobs = len(jobs)
        if totalJobs == 0:
            logging.info("No jobs associated to the workflow %s" % \
                         workflowSpecId)
            return

        # Kill all jobs (those not in the queue will be ignored)
        self.tqApi.killTasks(jobs)

        return
        

    def __avoidResubmission(self, jobSpecId):
        """
        Set number of executions to be equal to the maximum number of
        allowed retries so jobs will not be resubmitted, or even
        not submitted at all if they have not been submitted yet.
        """
        
        try:
            JobState.doNotAllowMoreSubmissions([jobSpecId])
        except ProdAgentException, ex:
            msg = "Updating max racers fields failed for job %s\n" % jobSpecId
            msg += str(ex)
            logging.error(msg)
            raise

        # remove all entries
        JobState.cleanout(jobSpecId)
    

    def eraseJob(self, jobSpecId):
        """
        Arguments:
          JobSpecId -- the job id.

        Return:
          none
        """
        logging.info("TaskQueueKiller.eraseJob(%s)" % jobSpecId)

        # kill job
        self.killJob(jobSpecId)

        # Avoid resubmission
        self.__avoidResubmission(jobSpecId)


    def eraseWorkflow(self, workflowSpecId):
        """
        Arguments:
          workflowSpecId -- the workflow id.

        Return:
          none
        """

        logging.info("TaskQueueKiller.eraseWorkflow(%s)" % workflowSpecId)
        
        # get job ids for workflows workflowSpecId
        jobs = JobState.retrieveJobIDs([workflowSpecId])
        jobs = map(lambda x: x[0], jobs)
        logging.debug("TaskQueueKiller-> jobs: %s" % str(jobs))

        totalJobs = len(jobs)
        if totalJobs == 0:
            logging.info("No jobs associated to the workflow %s" % \
                         workflowSpecId)
            return

        # Kill all jobs (those not in the queue will be ignored)
        self.tqApi.killTasks(jobs)

        # Avoid resubmission
        for job in jobs:
            self.__avoidResubmission(job)

        return


# register the killer plugin
registerKiller(TaskQueueKiller, TaskQueueKiller.__name__)

