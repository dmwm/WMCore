#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
# W6501: It doesn't like string formatting in logging messages
"""
The actual ASO tracker algorithm

This is pretty straightforward. Get a list of jobs we know are pendingaso, get their files,
query the central couchdb for their status. propagate it to the agent. boom boom.

"""
__all__ = []


import os.path
import threading
import logging
import traceback
import collections

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
import WMCore.Database.CMSCouch
CouchServer = WMCore.Database.CMSCouch.CouchServer

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.WMSpec.WMWorkload           import WMWorkload, WMWorkloadHelper
from WMCore.WMException                 import WMException
from WMCore.FwkJobReport.Report         import Report

class AsyncStageoutTrackerException(WMException):
    """
    The Exception class for the AsyncStageoutTrackerPoller

    """
    pass


class AsyncStageoutTrackerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()
        
        # Things needed to do this
        self.jobListAction = self.daoFactory(classname = "Jobs.GetAllJobs")

        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available    
        self.initAlerts(compName = "AsyncStageoutTracker")        
        
        return
    
    def setup(self, parameters = None):
        """
        Load DB objects required for queries
        """

        # For now, does nothing

        return

    def terminate(self, params):
        """
        _terminate_
        
        Do one pass, then commit suicide
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)

    def algorithm(self, parameters = None):
        """
    	Performs the handleErrors method, looking for each type of failure
    	And deal with it as desired.
        """
        pendingASOJobs = self.getJobsAction.execute(state = "asopending")
        
        self.beginTransaction()
        for job in pendingASOJobs:
            jobReport = Report()
            jobReportPath = job['fwjr_path']
            try:
                jobReportPath = jobReportPath.replace("file://","")
                jobReport.load( jobReportPath )
            except Exception, _:
                # if we got here, we must've used to have had a FWJR, knock it back
                # to the JobAccountant, they can deal with it
                logging.info( "ASOTracker: %s has no FWJR, but it should if we got here" % job['id'])
                self.stateChanger.propagate(job, "complete", "asopending")
                continue
            
            
            # FIXME get the right bits to query ASO for the proper file status
            # FIXME FIXME Also, get the right bits to query ASO for the new PFN
            #    I'm having trouble deploying ASO using the standard manage tools
            #    so I'll have to figure out what's broken with it before I can 
            #    change or test this
                       
            allFiles = job.getAllFileRefs()
            
            # Look through each job state and update it
            for fwjrFile in allFiles:
                if getattr(fwjrFile, "asyncDest", None) and \
                    not getattr(fwjrFile, "asyncStatus", None):
                    fwjrFile['asyncStatus'] = 'Success'
            
            # Save the FJR
            jobReport.save( jobReportPath )
            
            # Obviously need to change this to query the info from ASO
            #   if a job failed, send it to asofailed instead
            self.stateChanger.propagate(job, "complete", "asopending")
            
            # FIXME the above code doesn't change the LFN or check the file state
            # FIXME FIXME