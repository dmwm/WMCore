#!/usr/bin/env python

"""
_AsyncStageoutTracker_

Tracks jobs that need their outputs transferred via ASO and updates
the state machine when the transfers succeed/fail

"""





import logging
import threading

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness


from WMComponent.AsyncStageoutTracker.AsyncStageoutTrackerPoller import AsyncStageoutTrackerPoller

class AsyncStageoutTracker(Harness):
    """
    _AsyncStageoutTracker_

    Looks at jobs in the pendingaso state, moves them to failedaso/completeaso state when
    the central database reports things are okay
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.config = config

    def preInitialization(self):
        """
        Initializes plugins for different messages
        """

        # Add event loop to worker manager
        myThread = threading.currentThread()
        pollInterval = self.config.AsyncStageoutTracker.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(AsyncStageoutTrackerPoller(self.config), pollInterval)

