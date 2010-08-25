#!/usr/bin/env python

"""
_ErrorHandler_

The error handler pools for error conditions (CreateFailed, SubmitFailed, and JobFailed)
By looking at wmbs_job table's status filed.
All the jobs are handled respectively.

the different failure handlers are configurable in the config file and 
relate to the three stages of a job: create, submit, run 

The component runs in Poll mode, basically submits itself "Poll" message at the end of each cycle, so that it keeps polling
We can introduce some delay in polling, if have to.
"""

__revision__ = "$Id: ErrorHandler.py,v 1.7 2009/05/12 11:13:12 afaq Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "fvlingen@caltech.edu"


import logging

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory

class ErrorHandler(Harness):
    """
    _ErrorHandler_

    The error handler pools for error conditions (CreateFailed, SubmitFailed, and JobFailed)
    By looking at wmbs_job table's status filed.
    All the errors are handled respectively by handlers related to 
    the three stages of a job: create, submit, run 
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

    def preInitialization(self):
        """
        Initializes plugins for different messages
        """

        # Add event loop to worker manager
        myThread = threading.currentThread()
        pollInterval = self.config.ErrorHandler.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(WorkflowManagerPoller(), \
                                               pollInterval)

