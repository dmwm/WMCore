#!/usr/bin/env python
"""
Default function for Job Maker
"""
from __future__ import print_function
__all__ = []




from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

import exceptions
import threading



class MakeJob(BaseHandler):
    """
    Default handler for Make Job Events.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.

        self.nThreads = 5

        self.threadpool = ThreadPool(\
            "WMCore.WMSpec.Makers.Handlers.MakeJobSlave", \
            self.component, 'MakeJob', \
            self.nThreads)
        myThread = threading.currentThread()
        myThread.msgService.purgeMessages()

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        self.threadpool.enqueue(event, payload)
        print("Thread with payload " + str(payload) + " is enqueued")
