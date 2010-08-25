#!/usr/bin/env python
"""
ParentSlave handler from which other handlers
inherit.
"""
__all__ = []



import threading
import logging 

from WMCore.ThreadPool.ThreadSlave import ThreadSlave
from WMCore.WMFactory import WMFactory

class ParentSlave(ThreadSlave):
    """
    ParentSlave handler from which other handlers
    inherit.
    """
    # we initialize in the initInThread method to ensure
    # the initialization happens in its own thread and not
    # in the main thread.

    def __init__(self):
        ThreadSlave.__init__(self)
#        self.initInThread()

        
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
           "TQComp.Database."+myThread.dialect)
        # make sure you do not overload attributes
        # defined in your parent class
        self.queries = factory.loadObject("Queries")

        logging.debug("ParentSlave initialized")


    # this we overload from the base handler
    def __call__(self, parameters):
        """
        Handles the event with payload. The events are typically
        failure events with the payload the job id.
        """

        logging.debug("TaskHandler:New event: %s" % (parameters['event']))

        # The message handling is done in the extending class
#        # Now handle the message
#        myThread = threading.currentThread()
#        if parameters['event'] in ['NewTask']:

#           # Extract the task attributes
#           # Here we should check that all arguments are given correctly...
#           parts = parameters["payload"].split(",")  
#           logging.debug('TaskHandler:NewTask:parts'+str(parts))

#           # Insert job and its characteristics in the database
#           myThread.transaction.begin()
#           self.queries.add(*parts)

#           # Say how many we have
#           print "Number of tasks in queue: %s" % (self.queries.count())
#           myThread.transaction.commit()

#        else:
#            # unexpected message, scream!
#            pass
