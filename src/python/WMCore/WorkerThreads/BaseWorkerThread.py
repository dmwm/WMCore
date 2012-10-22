#!/usr/bin/env python
#pylint: disable-msg=R0902,R0201,W0613,W0703,E1102
"""
_BaseWorkerThread_

Base class for all regular worker threads managed by WorkerThreadManager.
Deriving classes should override algorithm, and optionally setup and terminate
to perform thread-specific setup and clean-up operations
"""



import threading
import logging
import time
import traceback
import sys

from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

from WMCore.Alerts import API as alertAPI

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
        # this will be set when addWorker is called in WorkerThreadManager
        self.component = None
        self.args = {}
        self.heartbeatAPI = None

        # Termination callback function
        self.terminateCallback = None

        # Init alert system
        self.sender = None
        self.sendAlert = None

        # Get the current DBFactory
        myThread = threading.currentThread()
        self.dbFactory = myThread.dbFactory

        # Get the logger
        self.logger = myThread.logger

        # get the procid from the mainthread msg service
        # if we use this in testing it might not be there.
        self.procid = 0
        if hasattr(myThread, 'msgService'):
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

        If this method raises an exception all workers will be terminated
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
        myThread.name = self.__class__.__name__
        myThread.dbFactory = self.dbFactory

        # Now we're in our own thread, set the logger
        myThread.logger = self.logger

        (connectDialect, junk) = self.component.config.CoreDatabase.connectUrl.split(":", 1)

        if connectDialect.lower() == "mysql":
            myThread.dialect = "MySQL"
        elif connectDialect.lower() == "oracle":
            myThread.dialect = "Oracle"
        elif connectDialect.lower() == "sqlite":
            myThread.dialect = "SQLite"

        logging.info("Initialising default database")
        myThread.dbi = myThread.dbFactory.connect()
        logging.info("Initialising default transaction")
        myThread.transaction = Transaction(myThread.dbi)

        # Call worker setup
        self.setup(parameters)
        myThread.transaction.commit()

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

            # heartbeat needed to be called after self.initInThread
            # to get the right name
            myThread = threading.currentThread()

            if hasattr(self.component.config, "Agent"):
                if getattr(self.component.config.Agent, "useHeartbeat", True):
                    self.heartbeatAPI.updateWorkerHeartbeat(myThread.getName())
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
                            # heartbeat needed to be called after self.initInThread
                            # to get the right name
                            if hasattr(self.component.config, "Agent"):
                                if getattr(self.component.config.Agent, "useHeartbeat", True):
                                    self.heartbeatAPI.updateWorkerHeartbeat(
                                        myThread.getName(), "Running")
                            self.algorithm(parameters)

                            # Catch if someone forgets to commit/rollback
                            if myThread.transaction.transaction is not None:
                                msg = """ Thread %s:  Transaction reached
                                          end of poll loop.""" % myThread.getName()
                                msg += " Raise a bug against me. Rollback."
                                logging.error(msg)
                                myThread.transaction.rollback()
                        except Exception, ex:
                            if myThread.transaction.transaction is not None:
                                myThread.transaction.rollback()
                            msg = "Error in worker algorithm (1):\nBacktrace:\n "
                            msg += (" %s %s" % (str(self), str(ex)))
                            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
                            for stackFrame in stackTrace:
                                msg += stackFrame

                            logging.error(msg)
                            # force entire component to terminate
                            try:
                                self.component.prepareToStop()
                            except Exception, ex:
                                logging.error("Failed to halt component after worker crash: %s" % str(ex))

                            if hasattr(self.component.config, "Agent"):
                                if getattr(self.component.config.Agent, "useHeartbeat", True):
                                    self.heartbeatAPI.updateWorkerError(
                                        myThread.getName(), msg)
                            raise ex
                        # Put the thread to sleep
                        time.sleep(self.idleTime)

            # Call specific thread termination method
            self.terminate(parameters)
        except Exception, ex:
            # Notify error
            msg = "Error in event loop (2): %s %s\nBacktrace:\n"
            msg = msg % (str(self), str(ex))
            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
            for stackFrame in stackTrace:
                msg += stackFrame
            logging.error(msg)

        # Indicate to manager that thread is done
        self.terminateCallback(threading.currentThread().name)

        # All done
        msg = "Worker thread %s terminated" % str(self)
        logging.info(msg)


    def initAlerts(self, compName = None):
        """
        _initAlerts_

        Setup the alerts for the rest of the system.

        sender: instance of the Alert messages Sender
        sendAlert: the code what sends the actual Alerts
            (documented in WMCore/Alerts/APIgetSendAlert)

        note:
            Tests are done in the API_t belonging to Alerts fw.
            This particular method is called from a number of
                components and some have particular tests on alerts sending.
        """
        if not compName:
            compName = self.__class__.__name__
        preAlert, sender = alertAPI.setUpAlertsMessaging(self,
                                                         compName = compName)
        sendAlert = alertAPI.getSendAlert(sender = sender,
                                          preAlert = preAlert)
        self.sender = sender
        self.sendAlert = sendAlert


    def __del__(self):
        """
        Unregister itself with Alert Receiver.

        """
        if self.sender:
            self.sender.unregister()
