#!/usr/bin/env python
"""
_BaseWorkerThread

Base class for all regular worker threads managed by WorkerThreadManager.
Deriving classes should override algorithm, and optionally setup and terminate
to perform thread-specific setup and clean-up operations
"""

__revision__ = "$Id: BaseWorkerThread.py,v 1.1 2009/02/01 11:03:36 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import threading
import logging

class BaseWorkerThread:
    """
    A base class for worker threads, used for work that needs to occur at
    regular intervals.
    """
    def __init__(self):
        """
        Creates the worker, called from parent thread
        """
        self.frequency = None
        self.terminate = None
        self.pause = None
        self.resume = None
        
        # Reference to the owner component
        self.component = None 
        
        # Get the current DBFactory
        myThread = threading.currentThread()
        self.dbFactory = myThread.dbFactory
        
        # get the procid from the mainthread msg service
        # if we use this in testing it might not be there.
        self.procid = 0
        if hasattr(myThread,'msgService'):
            self.procid = myThread.msgService.procid
    
    def __call__(self, parameters):
        """
        Thread entry point; handles synchronisation with run and terminate
        conditions
        """
        logging.info("Started running new worker thread")
        
        # Call specific thread startup method
        self.setup(parameters)
        
        # Run event loop while termination is not flagged
        while not self.terminate.isSet():
            # Check manager hasn't paused threads
            if self.pause.isSet():
                self.resume.wait()
            else:
                # Catch case where threads were paused and then terminated
                #  - threads should not run in this case!
                if not self.terminate.isSet():
                    # Do some work!
                    try:
                        self.doWork(parameters)
                    except Exception, ex:
                        logging.error("Error in worker thread: %s" % str(ex))
            
                    # Put the thread to sleep
                    time.sleep(self.frequency)
                    
        # Call specific thread termination method
        self.terminate(parameters)
        
        #ÊAll done!
        logging.info("Worker thread terminated")
    
    def setup(self, parameters):
        """
        Called when thread is being run for the first time. Optional in derived
        classes.
        """
        pass
    
    def terminate(self, parameters):
        """
        Called when thread is being terminated. Optional in derived classes.
        """
        pass
    
    def algorithm(self, parameters):
        """
        The method that performs the required work. Should be overridden in
        derived classes.
        """
        logging.error("Calling algorithm on BaseWorkerThread: Override me!")
