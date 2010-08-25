#!/usr/bin/env python
#pylint: disable-msg=R0902,R0201,W0613,W0703,E1102
"""
_BaseWorkerThread_

Base class for all regular worker threads managed by WorkerThreadManager.
Deriving classes should override algorithm, and optionally setup and terminate
to perform thread-specific setup and clean-up operations
"""

__revision__ = \
        "$Id: BaseWorkerThread.py,v 1.15 2009/08/13 00:05:16 meloam Exp $"
__version__ = "$Revision: 1.15 $"
__author__ = "james.jackson@cern.ch"

import threading
import logging
import time

from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

class BaseWorkerThread:
    """
    A base class for worker threads, used for work that needs to occur at
    regular intervals. Framework (through WorkerThreadManager) ensures that
    a default transaction, trigger and message service are available as in
    event handler threads.
    """
    def __init__(self):
        """
        Creates the worker, called from parent thread
        """
        self.idleTime = None
        self.notifyTerminate = None
        self.notifyPause = None
        self.notifyResume = None
        
        # Reference to the owner component and arguments
        self.component = None
        self.args = {}
        
        # Termination callback function
        self.terminateCallback = None
        
        # Get the current DBFactory
        myThread = threading.currentThread()
        self.dbFactory = myThread.dbFactory
        
        # Get the logger
        self.logger = myThread.logger
        
        # get the procid from the mainthread msg service
        # if we use this in testing it might not be there.
        self.procid = 0
        if hasattr(myThread,'msgService'):
            self.procid = myThread.msgService.procid
    
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
    
    def initInThread(self, parameters):
        """
        Called when the thread is actually running in its own thread. Performs
        internal object setup.
        """
        # Get the DB Factory we were passed by parent thread and assign to this
        # thread
        myThread = threading.currentThread()
        myThread.dbFactory = self.dbFactory
        
        # Now we're in our own thread, set the logger
        myThread.logger = self.logger

        # Set up database connection and default transaction
        #if self.component.config.CoreDatabase.dialect == 'mysql':
        #    myThread.dialect = 'MySQL'
        myThread.dialect = self.component.config.CoreDatabase.dialect

        logging.info("Initialising default database")
        myThread.dbi = myThread.dbFactory.connect()
        logging.info("Initialise transaction dictionary")
        myThread.transactions = {}
        logging.info("Initialising default transaction")
        myThread.transaction = Transaction(myThread.dbi)
        
        # Set up message service and trigger
        logging.info("Instantiating message queue for thread")
        factory = WMFactory("msgService", "WMCore.MsgService."+ \
            myThread.dialect)
        # we instantiate a message service here but we do not register it.
        # the main thread represents us in the msg service. We copy the 
        # the main thread procid to our object.
        myThread.msgService = factory.loadObject("MsgService")
        myThread.msgService.procid = self.procid
        msg = "Instantiating trigger service for thread"
        logging.info(msg)
        WMFactory("trigger", "WMCore.Trigger")
        myThread.trigger = myThread.factory['trigger'].loadObject("Trigger")
        # TODO: add trigger instantiation.
        
        # Call worker setup
        self.setup(parameters)
    
    def __call__(self, parameters):
        """
        Thread entry point; handles synchronisation with run and terminate
        conditions
        """
        try:
            msg = "Initialising worker thread %s" % str(self)
            logging.info(msg)
            
            # Call thread startup method
            self.initInThread(parameters)
            
            msg = "Worker thread %s started" % str(self)
            logging.info(msg)
            
            # Run event loop while termination is not flagged
            while not self.notifyTerminate.isSet():
                # Check manager hasn't paused threads
                if self.notifyPause.isSet():
                    self.notifyResume.wait()
                else:
                    # Catch case where threads were paused and then terminated
                    #  - threads should not run in this case!
                    if not self.notifyTerminate.isSet():
                        # Do some work!
                        try:
                            self.algorithm(parameters)
                        except Exception, ex:
                            msg = "Error in worker algorithm:"
                            msg += (" %s %s" % (str(self), str(ex)))
                            logging.error(msg)
                
                        # Put the thread to sleep
                        time.sleep(self.idleTime)
                        
            # Call specific thread termination method
            self.terminate(parameters)
        except Exception, ex:
            # Notify error
            msg = "Error in event loop: %s %s"
            msg = msg % (str(self), str(ex))
            logging.error(msg)
        
        # Indicate to manager that thread is done
        self.terminateCallback(threading.currentThread().name)
            
        # All done
        msg = "Worker thread %s terminated" % str(self)
        logging.info(msg)
