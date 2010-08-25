#!/usr/bin/env python
"""
Default handler for add dataset to watch
"""
__all__ = []
__revision__ = "$Id: DefaultAddDatasetWatch.py,v 1.2 2009/05/21 14:45:23 riahi Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class DefaultAddDatasetWatch(BaseHandler):
    """
    Default handler for dataset watch
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool to handle messages
        self.threadpool = ThreadPool(\
            "WMComponent.FeederManager.Handler.DefaultAddDatasetWatchSlave", \
            self.component, 'AddDatasetWatch', \
            self.component.config.FeederManager.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.

        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload['payload']})
