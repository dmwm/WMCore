#!/usr/bin/env python
"""
Default handler for submit failures.
"""
__all__ = []
__revision__ = "$Id: DefaultSubmit.py,v 1.1 2009/05/11 16:49:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class DefaultSubmit(BaseHandler):
    """
    Default handler for submit failures.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.
        self.threadpool = ThreadPool(\
            "WMComponent.ErrorHandler.Handler.DefaultSubmitSlave", \
            self.component, 'SubmitFailure', \
            self.component.config.ErrorHandler.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload})


