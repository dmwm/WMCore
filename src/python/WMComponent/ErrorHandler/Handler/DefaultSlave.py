#!/usr/bin/env python
"""
DefaultSlave handler from which other handlers
inherit.
"""
__all__ = []
__revision__ = "$Id: DefaultSlave.py,v 1.1 2009/02/06 15:26:35 fvlingen Exp $"
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
        if parameters['event'] in \
            ['SubmitFailure', 'CreateFailure', 'RunFailure']:
            # update the number of retries
            myThread.transaction.begin()
            currentRetries = self.errorQuery.update(parameters['payload'])
            if currentRetries >= int(self.component.config.ErrorHandler.maxRetries) :
                # remove from our errorhandler queue
                self.errorQuery.remove(parameters['payload'])
                # publish a general failure event.
                msg = {'name' : 'FinalJobFailure', \
                       'payload' : parameters['payload']}
                # remember the publish does not immediately publish
                # we need to call finish in the end. Calling the finish
                # should happen in the derived classes as the class might want 
                # to do something before publishing this message.
                # this pre-stages the message.
                myThread.msgService.publish(msg)
            myThread.transaction.commit()
        elif parameters['event'] == 'JobSuccess':
            # job was successful, remove from our table.
            myThread.transaction.begin()
            self.errorQuery.remove(parameters['payload'])
            myThread.transaction.commit()

