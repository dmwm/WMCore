#!/usr/bin/env python

"""
Auto generated stub be careful with editing,
Inheritance is preferred.
"""

import threading

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness

from WMComponent.JobEmulator.Handler.JobEmulator_TrackJobs import JobEmulator_TrackJobs
from WMComponent.JobEmulator.Handler.JobEmulator_JobSubmit import JobEmulator_JobSubmit
from WMComponent.JobEmulator.Handler.JobEmulator_Reset import JobEmulator_Reset
from WMComponent.JobEmulator.Handler.EmulateJob import EmulateJob

from WMComponent.JobEmulator.WorkerThread.JobEmulator_Update import JobEmulator_Update
from WMComponent.JobEmulator.WorkerThread.JobEmulator_TrackerUpdate import JobEmulator_TrackerUpdate

class JobEmulator(Harness):


    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)


    def preInitialization(self):
    # mapping from message types to handlers
        self.messages['EmulateJob'] = EmulateJob(self)
        self.messages['JobEmulator:Reset'] = JobEmulator_Reset(self) 
        # the job submit, and Track job are only activated if we do in-situ simulation:
        if self.config.JobEmulator.direct:
            self.messages['JobEmulator:TrackJob'] = JobEmulator_TrackJobs(self)
            self.messages['JobEmulator:JobSubmit'] = JobEmulator_JobSubmit(self)


    # IMPORTANT: worker threads need to be started in postInitialization
    # otherwise they do not get the proper reference for the message service procid.
    def postInitialization(self):
    # start the periodic checking of simulated job status and if defined, 
    # activate the job emulator placebo tracker using worker threads
        jobEmulatorUpdate = JobEmulator_Update(self)
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(jobEmulatorUpdate, 5)
        if self.config.JobEmulator.direct:
            jobEmulatorTrackerUpdate = JobEmulator_TrackerUpdate(self)
            myThread.workerThreadManager.addWorker(jobEmulatorTrackerUpdate, 5)
