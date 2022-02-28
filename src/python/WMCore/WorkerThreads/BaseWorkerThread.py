#!/usr/bin/env python
# pylint: disable=R0902,R0201,W0703,E1102
"""
_BaseWorkerThread_

Base class for all regular worker threads managed by WorkerThreadManager.
Deriving classes should override algorithm, and optionally setup and terminate
to perform thread-specific setup and clean-up operations
"""

from builtins import object
import logging
import sys
import threading
import time
import traceback

from WMCore.Database.DBExceptionHandler import db_exception_handler
from WMCore.Database.Transaction import Transaction


class BaseWorkerThread(object):
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

        # Init heartbeat flag and worker name
        self.useHeartbeat = False
        self.workerName = None

        # Init the timing
        self.lastTime = time.time()

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
        If inherited, then derived class has to call this method.
        """
        msg = "Thread gracefully terminated at %s" % time.strftime("%Y-%m-%dT%H:%M:%S")
        logging.info(msg)
        if self.useHeartbeat:
            self.heartbeatAPI.updateWorkerError(self.workerName, msg)

    def algorithm(self, parameters):
        """
        The method that performs the required work. Should be overridden in
        derived classes.

        If this method raises an exception all workers will be terminated
        """
        logging.error("Calling algorithm on BaseWorkerThread: Override me!")

    def setUpHeartbeat(self, myThread):
        # heartbeat needed to be called in self.initInThread
        # to get the right name but before the self.setup
        if hasattr(self.component.config, "Agent"):
            self.useHeartbeat = getattr(self.component.config.Agent, "useHeartbeat", True)
            self.workerName = myThread.getName()

        if self.useHeartbeat:
            self.heartbeatAPI.registerWorker(self.workerName)
        return

    def setUpLogDB(self, myThread):
        # setup logDB
        if hasattr(self.component.config, "General") and \
                hasattr(self.component.config.General, "central_logdb_url") and \
                hasattr(self.component.config, "Agent"):
            from WMCore.Services.LogDB.LogDB import LogDB
            myThread.logdbClient = LogDB(self.component.config.General.central_logdb_url,
                                         self.component.config.Agent.hostName, logger=logging)
        else:
            myThread.logdbClient = None
        return

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

        (connectDialect, _junk) = self.component.config.CoreDatabase.connectUrl.split(":", 1)

        if connectDialect.lower() == "mysql":
            myThread.dialect = "MySQL"
        elif connectDialect.lower() == "oracle":
            myThread.dialect = "Oracle"

        logging.info("Initialising default database")
        myThread.dbi = myThread.dbFactory.connect()
        logging.info("Initialising default transaction")
        myThread.transaction = Transaction(myThread.dbi)

        self.setUpHeartbeat(myThread)
        self.setUpLogDB(myThread)

        # Call worker setup
        self.setup(parameters)
        myThread.transaction.commit()

    def __call__(self, parameters):
        """
        Thread entry point; handles synchronisation with run and terminate
        conditions
        """
        errMsg = ""
        try:
            msg = "Initialising worker thread %s" % str(self)
            logging.info(msg)

            # Call thread startup method
            self.initInThread(parameters)

            msg = "Worker thread %s started" % str(self)
            logging.info(msg)

            myThread = threading.currentThread()

            # Run event loop while termination is not flagged
            algorithmWithDBExceptionHandler = db_exception_handler(self.algorithm)
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
                            if self.useHeartbeat:
                                self.heartbeatAPI.updateWorkerHeartbeat(self.workerName, "Running")

                            tSpent, results, _ = algorithmWithDBExceptionHandler(parameters)
                            if tSpent and self.useHeartbeat:
                                logging.info("%s took %.3f secs to execute", self.workerName, tSpent)
                                self.heartbeatAPI.updateWorkerCycle(self.workerName, tSpent, results)

                            # Catch if someone forgets to commit/rollback
                            if myThread.transaction.transaction is not None:
                                msg = """ Thread %s:  Transaction reached
                                          end of poll loop.""" % self.workerName
                                msg += " Raise a bug against me. Rollback."
                                logging.error(msg)
                                myThread.transaction.rollback()
                        except Exception as ex:
                            if myThread.transaction.transaction is not None:
                                myThread.transaction.rollback()
                            errMsg = "Error in worker algorithm (1):\nBacktrace:\n "
                            errMsg += (" %s %s" % (str(self), str(ex)))
                            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
                            for stackFrame in stackTrace:
                                errMsg += stackFrame
                            logging.error(errMsg)
                            # force entire component to terminate
                            try:
                                self.component.prepareToStop()
                            except Exception as ex1:
                                logging.error("Failed to halt component after worker crash: %s", str(ex1))
                            raise ex
                        # Put the thread to sleep
                        self.sleepThread()

            # Call specific thread termination method
            self.terminate(parameters)
        except Exception as ex:
            # Notify error
            msg = "Error in event loop (2): %s %s\nBacktrace:\n"
            msg = msg % (str(self), str(ex))
            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
            for stackFrame in stackTrace:
                msg += stackFrame
            logging.error(msg)
            # send heartbeat message that thread is terminated
            if self.useHeartbeat:
                msg = errMsg or msg
                try:
                    self.heartbeatAPI.updateWorkerError(self.workerName, errMsg)
                except Exception as ex:
                    logging.error("Heartbeat error update failed %s", str(ex))

        # Indicate to manager that thread is done
        self.terminateCallback(threading.currentThread().name)

        # All done
        msg = "Worker thread %s terminated" % str(self)
        logging.info(msg)

    def sleepThread(self):
        """
        _sleepThread_

        A subclassable method to make the thread sleep.

        The default (naiive) time.sleep(self.idleTime) isn't always
        the best idea, let different workers do it differently.

        Wakes the thread up every minute for a quick heartbeat.
        Need to constantly watch if the thread is terminated for
        properly stopping/terminating it.

        returns control when it's time to wake back up
        doesn't return any values
        """
        idleTime = self.idleTime
        while idleTime > 0:
            if self.useHeartbeat and idleTime % 60 == 0:
                # send a heartbeat every minute
                self.heartbeatAPI.updateWorkerHeartbeat(self.workerName, "Running")

            if self.notifyTerminate.isSet():
                break

            time.sleep(1)
            idleTime -= 1
