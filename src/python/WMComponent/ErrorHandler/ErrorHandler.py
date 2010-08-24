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

__revision__ = "$Id: ErrorHandler.py,v 1.1 2008/09/12 13:02:09 fvlingen Exp $"
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

        # in case nothing was configured we have a fallback.
        if not hasattr(config.ErrorHandler, "submitFailureHandler"):
            logging.warning("Using default submit failure handler!")
            config.ErrorHandler.submitFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultSubmit'
        if not hasattr(config.ErrorHandler, "createFailureHandler"):
            logging.warning("Using default create failure handler!")
            config.ErrorHandler.createFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultCreate'
        if not hasattr(config.ErrorHandler, "runFailureHandler"):
            logging.warning("Using default run failure handler!")
            config.ErrorHandler.runFailureHandler =  \
                'WMComponent.ErrorHandler.Handler.DefaultRun'

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['SubmitFailure'] = \
            factory.loadObject(config.ErrorHandler.submitFailureHandler, self)
        self.messages['CreateFailure'] = \
            factory.loadObject(config.ErrorHandler.createFailureHandler, self)
        self.messages['RunFailure'] = \
            factory.loadObject(config.ErrorHandler.runFailureHandler, self)


