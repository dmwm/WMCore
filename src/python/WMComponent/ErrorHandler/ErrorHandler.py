#!/usr/bin/env python

"""
_ErrorHandler_

The ErrorHandler subcribes to Error events. The payload of an error event
varies. For a job failure event the payload consists of a job report
from which we extract the job spec id. Other error events will have
different payloads. Depending on the type of error event the appropiate
error handler will be loaded for handling the event. 

the different failure handlers are configurable in the config file and 
relate to the three stages of a job: create, submit, run 
"""

__revision__ = "$Id: ErrorHandler.py,v 1.3 2008/09/30 18:25:38 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"
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
    
    The ErrorHandler subcribes to Error events. The payload of an error event
    varies. For a job failure event the payload consists of a job report
    from which we extract the job spec id. Other error events will have
    different payloads. Depending on the type of error event the appropiate
    error handler will be loaded for handling the event. 
    
    the different failure handlers are configurable in the config file and 
    relate to the three stages of a job: create, submit, run 
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

    def preInitialization(self):
        # in case nothing was configured we have a fallback.
        if not hasattr(self.config.ErrorHandler, "submitFailureHandler"):
            logging.warning("Using default submit failure handler!")
            self.config.ErrorHandler.submitFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultSubmit'
        if not hasattr(self.config.ErrorHandler, "createFailureHandler"):
            logging.warning("Using default create failure handler!")
            self.config.ErrorHandler.createFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultCreate'
        if not hasattr(self.config.ErrorHandler, "runFailureHandler"):
            logging.warning("Using default run failure handler!")
            self.config.ErrorHandler.runFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultRun'
        if not hasattr(self.config.ErrorHandler, "jobSuccessHandler"):
            logging.warning("Using default job success handler!")
            self.config.ErrorHandler.jobSuccessHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultSuccess'

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['SubmitFailure'] = \
            factory.loadObject(self.config.ErrorHandler.submitFailureHandler, self)
        self.messages['CreateFailure'] = \
            factory.loadObject(self.config.ErrorHandler.createFailureHandler, self)
        self.messages['RunFailure'] = \
            factory.loadObject(self.config.ErrorHandler.runFailureHandler, self)
        self.messages['JobSuccess'] = \
            factory.loadObject(self.config.ErrorHandler.jobSuccessHandler, self)


