#!/usr/bin/env python

"""
Simulates handling job submit event. This handler is 
only used if the emulator is configured to handle submit events.

That is this handler uses a submitter plugin that can also 
be used by a job submitter. So either the jobsubmitter is used
(through the normal 'SubmitJob' event or this handler is used
through the JobEmulator:JobSubmit event.
"""

import os
import threading

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.WMFactory import WMFactory

#FIXME: needs to be replaced.
from ProdCommon.MCPayloads.JobSpec import JobSpec

class JobEmulator_JobSubmit(BaseHandler):
    """
    Simulates handling job submit event. This handler is 
    only used if the emulator is configured to handle submit events.
    
    That is this handler uses a submitter plugin that can also 
    be used by a job submitter. So either the jobsubmitter is used
    (through the normal 'SubmitJob' event or this handler is used
    through the JobEmulator:JobSubmit event.
    """

    def __init__(self, component):
        """
        Load proper submitter to be used by this handler.
        """
        BaseHandler.__init__(self, component)
        # check what submitter plugin we use. 
        # when the emulator also submits jobs.
        submitter = self.component.config.JobEmulator.submitter
        self.factory = WMFactory('factory','')
        self.submitter = self.factory.loadObject(submitter)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        The payload is a location to the jobspec file.
        """
        # payload is a jobspec file location.
 
        jobspec = JobSpec()
        jobspec.load(payload)
        self.submitter.specFiles = {}
        self.submitter.toSubmit = {}
        # is it bulk or not?
        msgs = []
        if not jobspec.isBulkSpec():
            jobId = jobspec.parameters['JobName']
            cacheDir = os.path.join(self.component.config.General.jobCache, jobId)         
            self.submitter.specFiles[jobId] = payload
            self.submitter.toSubmit[jobId] = cacheDir
            msg = {'name':'JobEmulator:TrackJob', 'payload': jobId}
            msgs.append(msg)      
        self.submitter.doSubmit()
 
        myThread = threading.currentThread()
        # if we use this handler it means we are in direct
        # simulation mode and hence we also need to publish
        # a JobEmulator:TrackJob.
        myThread.msgService.publish(msgs)


