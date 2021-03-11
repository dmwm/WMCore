#!/usr/bin/env python
# pylint: disable=E1101

"""
Harness class that wraps standard functionality used in all daemon
components including:

(1) Instantiating message service, trigger and other database classes
including session objects and workflow entities.

(2) Subscribing to default messages (e.g. logging and stopping server)

(3) Providing a method to test the component without a message service

(4) Instatiate the log master

(5) Provides basic uniform log information (useful for operators)

(6) Method to publish monitoring information

"""
from __future__ import print_function
from builtins import object

import logging
import os
import sys
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler

from WMCore import WMLogging
from WMCore.Agent.ConfigDBMap import ConfigDBMap
from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.WorkerThreads.WorkerThreadManager import WorkerThreadManager


class HarnessException(WMException):
    """
    _HarnessException_

    Well, it has the harness errors in it.
    Otherwise, it's just part of WMException.
    """


class Harness(object):
    """
    Harness class that wraps standard functionality used in all daemon
    components
    """

    def __init__(self, config, compName=None):
        """
        init

        The constructor is empty as we have an initalization method
        that can be called inside new threads (we use thread local attributes
        at startup.

        Default intialization of the harness including setting some diagnostic
        messages
        """
        self.config = config

        # component name is always the class name of child class
        if not compName:
            compName = self.__class__.__name__

        if compName not in (self.config.listComponents_() + self.config.listWebapps_()):
            raise WMException(WMEXCEPTION['WMCORE-8'] + compName, 'WMCORE-8')
        if not hasattr(self.config, "Agent"):
            self.config.section_("Agent")

        self.config.Agent.componentName = compName
        compSect = getattr(self.config, compName, None)
        if compSect is None:
            # Then we have a major problem - there's no section with this name
            logging.error("Could not find section %s in config", compName)
            logging.error("We are returning, and hoping you know what you're doing!")
            logging.debug("Config: %s", self.config)
            return
        # check if componentDir is set if not assign.
        if getattr(compSect, 'componentDir', None) is None:
            if not hasattr(self.config, "General"):
                # Don't do anything.  Assume the user knows what they are doing.
                logging.error("Missing componentDir and General section in config")
                logging.error("Going to trust you to know what you're doing.")
                return

            compSect.componentDir = os.path.join(self.config.General.workDir,
                                                 'Components',
                                                 self.config.Agent.componentName)
        # we have name and location of the log files. Now make sure there
        # is a directory.
        try:
            if not os.path.isdir(compSect.componentDir):
                os.makedirs(compSect.componentDir)
        except Exception as ex:
            logging.error("Encountered exception while making componentDirs: %s", str(ex))
            logging.error("Ignoring")

        self.threadManagerName = ''
        self.heartbeatAPI = None
        self.messages = {}
        self.logMsg = {}

        return

    def initInThread(self):
        """
        Default intialization of the harness including setting some diagnostic
        messages. This method is called when we call 'prepareToStart'
        """
        try:
            self.messages = {}

            compName = self.config.Agent.componentName
            compSect = getattr(self.config, compName, None)
            if not hasattr(compSect, "logFile"):
                if not getattr(compSect, 'componentDir', None):
                    errorMessage = "No componentDir for log entries found!\n"
                    errorMessage += "Harness cannot run without componentDir.\n"
                    logging.error(errorMessage)
                    raise HarnessException(errorMessage)
                compSect.logFile = os.path.join(compSect.componentDir, "ComponentLog")
            print('Log file is: ' + compSect.logFile)
            logHandler = RotatingFileHandler(compSect.logFile,
                                             "a", 1000000000, 3)
            logMsgFormat = getattr(compSect, "logMsgFormat",
                                   "%(asctime)s:%(thread)d:%(levelname)s:%(module)s:%(message)s")
            logFormatter = \
                logging.Formatter(logMsgFormat)
            logHandler.setFormatter(logFormatter)
            logLevelName = getattr(compSect, 'logLevel', 'INFO')
            logLevel = getattr(logging, logLevelName)
            logging.getLogger().addHandler(logHandler)
            logging.getLogger().setLevel(logLevel)
            self.logMsg = {'DEBUG': logging.DEBUG,
                           'ERROR': logging.ERROR,
                           'NOTSET': logging.NOTSET,
                           'CRITICAL': logging.CRITICAL,
                           'WARNING': logging.WARNING,
                           'INFO': logging.INFO,
                           'SQLDEBUG': logging.SQLDEBUG}
            if hasattr(compSect, "logLevel") and compSect.logLevel in self.logMsg:
                logging.getLogger().setLevel(self.logMsg[compSect.logLevel])
            WMLogging.sqldebug("wmcore level debug:")

            # If not previously set, force wmcore cache to current path
            if not os.environ.get('WMCORE_CACHE_DIR'):
                os.environ['WMCORE_CACHE_DIR'] = os.path.join(compSect.componentDir, '.wmcore_cache')

            logging.info(">>>Starting: " + compName + '<<<')
            # check which backend to use: MySQL, Oracle, etc... for core
            # services.
            # we recognize there can be more than one database.
            # be we offer a default database that is used for core services.
            logging.info(">>>Initializing default database")
            logging.info(">>>Check if connection is through socket")
            myThread = threading.currentThread()
            myThread.logger = logging.getLogger()
            logging.info(">>>Setting config for thread: ")
            myThread.config = self.config

            logging.info(">>>Building database connection string")
            # check if there is a premade string if not build it yourself.
            dbConfig = ConfigDBMap(self.config)
            dbStr = dbConfig.getDBUrl()
            options = dbConfig.getOption()
            # we only want one DBFactory per database so we will need to
            # to pass this on in case we are using threads.
            myThread.dbFactory = DBFactory(myThread.logger, dbStr, options)

            myThread.sql_transaction = True
            if myThread.dbFactory.engine:

                myThread.dbi = myThread.dbFactory.connect()
                myThread.transaction = Transaction(myThread.dbi)

            else:

                myThread.dbi = myThread.config.CoreDatabase.connectUrl
                myThread.sql_transaction = False

            # Attach a worker manager object to the main thread
            if not hasattr(myThread, 'workerThreadManager'):
                myThread.workerThreadManager = WorkerThreadManager(self)
            else:
                myThread.workerThreadManager.terminateSlaves.clear()
            myThread.workerThreadManager.pauseWorkers()

            logging.info(">>>Initialize transaction dictionary")

            (connectDialect, dummy) = dbStr.split(":", 1)

            if connectDialect.lower() == 'mysql':
                myThread.dialect = 'MySQL'
            elif connectDialect.lower() == 'oracle':
                myThread.dialect = 'Oracle'

            logging.info("Harness part constructor finished")
        except Exception as ex:
            logging.critical("Problem instantiating " + str(ex))
            logging.error("Traceback: %s", str(traceback.format_exc()))
            raise

    def preInitialization(self):
        """
        _preInitialization_

        returns: nothing

        method that can be overloaded and will be called before the
        start component is called. (enables you to set message->handler
        mappings). You use the self.message dictionary of the base class
        to define the mappings.

        """
        pass

    def postInitialization(self):
        """
        _postInitialization_

        returns: nothing

        method that can be overloaded and will be called after the start
        component does the standard initialization, but before the wait
        (enables you to publish events when starting up)

        Define actions you want to execute before the actual message
        handling starts. E.g.: publishing some messages, or removing
        messages.

        """
        pass

    def logState(self):
        """
        _logState_

        returns: string

        method that can be overloaded to log additional state information
        (should return atring)
        """
        msg = 'No additional state information for ' + \
              self.config.Agent.componentName
        return msg

    def publishItem(self, items):
        """
        _publishItem_

        returns: nothing

        A method that publishes a (dictionary) set or 1 item
        to a monitoring service.
        """
        # FIXME: do we need this method. If so we need to agree
        # FIXME: on some default monitoring publication mechanism.
        pass

    def __call__(self, event, payload):
        """
        Once upon a time this was for doing the handling of diagnostic messages

        With the test-deprecating of the MsgService based diagnostics, we've basically
        scratched this.

        I'm leaving this in so at least the framework is still there

        -mnorman
        """
        return

    def initialization(self):
        """
        _initialization__

        Used the handle initializing the MsgService.  The MsgService
        is no longer used.

        Removed but not deleted, since all sorts of things call it
        """
        return

    def prepareToStart(self):
        """
        _prepareToStart_

        returns: Nothing

        Starts the initialization procedure. It is mainly an aggregation method
        so it can easily used in tests.
        """
        self.state = 'initialize'
        self.initInThread()
        # note: every component gets a (unique) name:
        # self.config.Agent.componentName
        logging.info(">>>Registering Component - %s", self.config.Agent.componentName)

        if getattr(self.config.Agent, "useHeartbeat", True):
            self.heartbeatAPI = HeartbeatAPI(self.config.Agent.componentName)
            self.heartbeatAPI.registerComponent()

        logging.info('>>>Starting initialization')

        logging.info('>>>Setting default transaction')
        myThread = threading.currentThread()

        self.preInitialization()

        if myThread.sql_transaction:
            myThread.transaction.begin()

        self.initialization()
        self.postInitialization()

        if myThread.sql_transaction:
            myThread.transaction.commit()

        logging.info('>>>Committing default transaction')

        logging.info(">>>Starting worker threads")
        myThread.workerThreadManager.resumeWorkers()

        logging.info(">>>Initialization finished!\n")
        # wait for messages
        self.state = 'active'

    def prepareToStop(self, wait=False, stopPayload=""):
        """
        _stopComponent

        Stops the component, including all worker threads. Allows call from
        test framework
        """
        # Stop all worker threads
        logging.info(">>>Terminating worker threads")
        myThread = threading.currentThread()
        try:
            myThread.workerThreadManager.terminateWorkers()
        except Exception:
            # We may not have a thread manager
            pass

        if wait:
            logging.info(">>>Shut down of component while waiting for threads to finish")
            # check if nr of threads is specified.
            activeThreads = 1
            if stopPayload != "":
                activeThreads = int(stopPayload)
                if activeThreads < 1:
                    activeThreads = 1
            while threading.activeCount() > activeThreads:
                logging.info('>>>Currently ' + str(threading.activeCount()) + ' threads active')
                logging.info('>>>Waiting for less than ' + str(activeThreads) + ' to be active')
                time.sleep(5)

    def handleMessage(self, type='', payload=''):
        """
        __handleMessage_

        Formerly used to handle messages - now non-functional
        Left here in case someone else is using it (i.e. PilotManager)
        """
        return

    def startDaemon(self, keepParent=False, compName=None):
        """
        Same result as start component, except that the comopnent
        is started as a daemon, after which you can close your xterm
        and the process will still run.

        The keepParent option enables us to keep the parent process
        which is used during testing,
        """
        msg = "Starting %s as a daemon " % self.config.Agent.componentName
        print(msg)
        if not compName:
            compName = self.__class__.__name__
        compSect = getattr(self.config, compName, None)
        msg = "Log will be in %s " % compSect.componentDir
        print(msg)
        # put the daemon config file in the work dir of this component.
        # FIXME: this file will be replaced by a database table.
        compSect = getattr(self.config, self.config.Agent.componentName, None)
        pid = createDaemon(compSect.componentDir, keepParent)
        # if this is not the parent start the component
        if pid == 0:
            self.startComponent()
            # if this is the parent return control to the testing environment.

    def startComponent(self):
        """
        _startComponent_

        returns: Nothing

        Start up the component, performs initialization and waits indefinitely
        Calling this method results in the application
        running in the xterm (not in daemon mode)

        """
        myThread = threading.currentThread()
        try:
            msg = 'None'
            self.prepareToStart()
            while True:
                time.sleep(360)

        except Exception as ex:
            if self.state == 'initialize':
                errormsg = """PostMortem: choked when initializing with error: %s\n""" % (str(ex))
                stackTrace = traceback.format_tb(sys.exc_info()[2], None)
                for stackFrame in stackTrace:
                    errormsg += stackFrame
            else:
                errormsg = ""
                stackTrace = traceback.format_tb(sys.exc_info()[2], None)
                for stackFrame in stackTrace:
                    errormsg += stackFrame
                logging.error(errormsg)
                logging.error(">>>Fatal Error, Preparing to Rollback Transaction")
                if getattr(myThread, 'transaction', None) is not None:
                    myThread.transaction.rollback()
                self.prepareToStop(False)
                errormsg = """
PostMortem: choked while handling messages  with error: %s
while trying to handle msg: %s
                """ % (str(ex), str(msg))
            print(errormsg)
            logging.critical(errormsg)
            raise
        logging.info("System shutdown complete!")
        # this is to ensure exiting when in daemon mode.
        sys.exit()

    def __str__(self):
        """

        return: string

        String representation of the status of this component.
        """

        msg = 'Status of this component : \n'
        msg += '\n'
        msg += '>>Event Subscriptions --> Handlers<<\n'
        msg += '------------------------------------\n'
        for message in self.messages:
            msg += message + '-->' + str(self.messages[message]) + '\n'
        msg += '\n'
        msg += '\n'
        msg += '>>Parameters --> Values<<\n'
        msg += '-------------------------\n'
        msg += str(self.config)
        additionalMsg = self.logState()
        if additionalMsg != '':
            msg += '\n'
            msg += 'Additional state information\n'
            msg += '----------------------------\n'
            msg += '\n'
            msg += str(additionalMsg)
            msg += '\n'
        return msg
