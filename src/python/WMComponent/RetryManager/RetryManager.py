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

__revision__ = "$Id: RetryManager.py,v 1.1 2009/05/11 16:49:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"
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

        # in case nothing was configured we have a fallback.

        if not hasattr(self.config.ErrorHandler, "createFailureHandler"):
            logging.warning("Using default create failure handler!")
            self.config.ErrorHandler.createFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultCreate'
 

        if not hasattr(self.config.ErrorHandler, "submitFailureHandler"):
            logging.warning("Using default submit failure handler!")
            self.config.ErrorHandler.submitFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultSubmit'

       if not hasattr(self.config.ErrorHandler, "runFailureHandler"):
            logging.warning("Using default run failure handler!")
            self.config.ErrorHandler.runFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultRun'

        if  self.messages['RetryManager::Start']
	# Start the manager on this manager
 
        if  self.messages['RetryManager::Stop'] = \
	# Stop the manager on this message

	# ADD the code to let the Manager get into Polling Mode

