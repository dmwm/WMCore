 #!/usr/bin/env python
"""
_TaskQueueTracker_

Tracker for TaskQueue submissions.
"""

__revision__ = "$Id: TaskQueueTracker.py,v 1.1 2009/07/08 17:14:37 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import os

from CondorTracker.TrackerPlugin import TrackerPlugin
from CondorTracker.Registry import registerTracker
import ProdCommon.FwkJobRep.ReportState as ReportState

#from JobEmulator.JobEmulatorAPI import queryJobsByID
#from JobEmulator.JobEmulatorAPI import removeJob

from WMCore import Configuration as WMCoreConfig
from ProdAgentCore.Configuration import loadProdAgentConfiguration

from TQComp.Apis.TQStateApi import TQStateApi
from TQComp.Constants import taskStates


class TaskQueueTracker(TrackerPlugin):
    """
    _TaskQueueTracker_

    Poll the Job Emulator for tracking information

    """
    def __init__(self):
        TrackerPlugin.__init__(self)
        self.classads = None
        self.cooloff = "00:1:00"

        logging.debug("<<<<< Creating TaskQueueTracker>>>>>")

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
        self.tqApi = TQStateApi(logging, myconfig, None)
        logging.debug("<<<<< TaskQueueTracker created! >>>>>")

    def initialise(self):
        """
        _initialise_

        """
        pass
        
    def updateSubmitted(self, *submitted):
        """
        _updateSubmitted_

        Override to look at each submitted state job spec id provided
        and change its status if reqd.

        """
#        submitted = list(submitted)
        logging.info("TQTracker: Submitted Count: %s" % len(submitted))
#        logging.debug("TQTracker: Submitted items: %s" % (submitted))

        # Query for all tasks and then read one by one from the result
        states = self.tqApi.getStateOfTasks(submitted)

        logging.debug("TaskQueueTracker: Received states: %s" % states)

        toRemove = []
        for subId in submitted:
            try:
                jobState = states[subId]
            except:
                jobState = None

            # Job not known by TaskQueue, first check job report
            if jobState == None:
                msg = "No Status entry for %s, checking job report" % subId
                logging.debug(msg)
                jobState = self.jobReportStatus(subId)
                
            # If status still None, declare job lost/failed
            if jobState == None:
                self.TrackerDB.jobFailed(subId)
                logging.debug("Job %s has been lost" % (subId))
                continue

            # Still submitted, nothing to be done
            if jobState == taskStates["Queued"]:
                logging.debug("Job %s is pending" % (subId))
                continue

            # If running or completed already, forward to running handler
            elif (jobState == taskStates["Running"]) or \
                   (jobState == taskStates["Done"]):
                logging.debug("Job %s is running" % (subId))
                self.TrackerDB.jobRunning(subId)

            # Failed 
            elif jobState == taskStates["Failed"]:
                logging.debug("Job %s failed" % (subId))
                self.TrackerDB.jobFailed(subId)
                toRemove.append(subId)
                
            # No more states left!
            else:
                logging.error("Unknown job state: %s" % jobState)

        # Now remove all tasks that deserve so
        if toRemove:
            self.tqApi.removeTasksById(toRemove)
        
        return


    def updateRunning(self, *running):
        """
        _updateRunning_

        Check on Running Job

        """
#        running = list(running)

        logging.info("TQTracker: Running Count: %s" % len(running))

        # Query for all tasks and then read one by one from the result
        states = self.tqApi.getStateOfTasks(running)

        toRemove = []
        for runId in running:
            try:
                jobState = states[runId]
            except:
                jobState = None

            # Job not known by TaskQueue, first check job report
            if jobState == None:
                msg = "No Status entry for %s, checking job report" % runId
                logging.debug(msg)
                jobState = self.jobReportStatus(runId)
                
            # If status still None, declare job lost/failed
            if jobState == None:
                self.TrackerDB.jobFailed(runId)
                logging.debug("Job %s has been lost" % (runId))
                continue

            # Still running, nothing to do
            if jobState == taskStates["Running"]:
                logging.debug("Job %s is still running" % (runId))
                continue

            # Complete 
            elif jobState == taskStates["Done"]:
                self.TrackerDB.jobComplete(runId)
                logging.debug("Job %s complete" % (runId))
                toRemove.append(runId)

            # Failed 
            elif jobState == taskStates["Failed"]:
                logging.debug("Job %s failed" % (runId))
                self.TrackerDB.jobFailed(runId)
                toRemove.append(runId)
            
            # No more states left!
            else:
                logging.error("Unknown job state: %s" % jobState)
   
        # Now remove all tasks that deserve so
        self.tqApi.removeTasksById(toRemove)
        
        return



    def updateComplete(self, *complete):
        """
        _updateComplete_

        Take any required action on completion.

        Note: Do not publish these to the PA as success/failure, that
        is handled by the component itself

        """
        if len(complete) == 0:
            return
        summary = "Jobs Completed:\n"
        for compId in complete:
            summary += " -> %s\n" % compId
        logging.info(summary)
        return


    def updateFailed(self, *failed):
        """
        _updateFailed_

        Take any required action for failed jobs on completion

        """
        pass

        return

    def kill(self, *toKill):
        """
        _kill_

        """
        pass

    def cleanup(self):
        """
        _cleanup_

        """
        pass
        

    def findJobReport(self, jobSpecId):
        """
        _findJobReport_
        
        Given a job spec Id, find the location of the job report file if it exists.
        Return the path of the file.
        If not found, return None
        
        """
        cache = self.getJobCache(jobSpecId)
        if cache == None:
            logging.debug("No JobCache found for Job Spec ID: %s" % jobSpecId)
            return None
        reportFile = "%s/FrameworkJobReport.xml" % cache
        if not os.path.exists(reportFile):
            logging.debug("Report File Not Found: %s" % reportFile)
            return None
        return reportFile
    
    
    def jobReportStatus(self, jobSpecId):
        """
        _jobReportStatus_

        Find the job report and determine the status of the job if possible.
        Should return some task state if a status is available, if the file 
        cannot be found, return None

        """
        report = self.findJobReport(jobSpecId)
        if report == None:
            return None
        
        if ReportState.checkSuccess(report):
            return taskStates["Done"]
            
        # If not success, then it is a failure
        return taskStates["Failed"]

registerTracker(TaskQueueTracker, TaskQueueTracker.__name__)
