#!/usr/bin/env python
"""
Default handler for JobSuccess envents.
"""
__all__ = []
__revision__ = "$Id: DefaultSuccess.py,v 1.1 2009/05/11 16:49:05 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class DefaultSuccess(BaseHandler):
    """
    Default handler for dealing with job success
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.
        self.threadpool = ThreadPool(\
            "WMComponent.ErrorHandler.Handler.DefaultSlave", \
            self.component, 'JobSuccess', \
            self.component.config.ErrorHandler.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload})


