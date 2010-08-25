#!/usr/bin/env python
"""
DefaultSlave handler from which other handlers
inherit.
"""
__all__ = []
__revision__ = "$Id: DefaultSlave.py,v 1.1 2009/05/11 16:49:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.ThreadPool.ThreadSlave import ThreadSlave
from WMCore.WMFactory import WMFactory

class DefaultSlave(ThreadSlave):
    """
    DefaultSlave handler from which other handlers
    inherit.
    """
    # we initialize in the initInThread method to ensure
    # the initialization happens in its own thread and not
    # in the main thread.

    def initInThread(self):
        """
        Called during thread initialization. Loads the
        backend for this instance.
        """
        # make sure you instantiate the super class method.
        ThreadSlave.initInThread(self)
        
        # load queries for backend.
        myThread = threading.currentThread()
        factory = WMFactory("default", \
        "WMComponent.ErrorHandler.Database."+myThread.dialect)
        # make sure you do not overload attributes
        # defined in your parent class
        self.errorQuery = factory.loadObject("Queries")


    # this we overload from the base handler
    def __call__(self, parameters):
        """
        Handles the event with payload. The events are typically
        failure events with the payload the job id.
        """
        # FIXME: if the payload is the job report location we
        # need to read that to retrieve the job id.

        myThread = threading.currentThread()
        myThread.msgService.publish(msg)
        myThread.transaction.commit()

