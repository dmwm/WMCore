#!/usr/bin/env python
"""
Default handler for create failures.
"""
__all__ = []
__revision__ = "$Id: DefaultCreate.py,v 1.1 2008/10/08 21:19:34 afaq Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class DefaultCreate(BaseHandler):
    """
    Default handler for create failures.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.
        self.threadpool = ThreadPool(\
            "WMComponent.ErrorHandler.Handler.DefaultCreateSlave", \
            self.component, 'CreateFailure', \
            self.component.config.ErrorHandler.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        self.threadpool.enqueue(event, payload)


