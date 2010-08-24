#!/usr/bin/env python
"""
Base handler for NewTask.
"""
__all__ = []



from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class TaskHandler(BaseHandler):
    """
    Base handler for new tasks.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.
        self.threadpool = ThreadPool(\
            "TQComp.Handler.TaskHandlerSlave", \
            self.component, 'default', \
            self.component.config.TQComp.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload})


