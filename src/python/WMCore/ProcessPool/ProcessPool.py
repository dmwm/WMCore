#!/usr/bin/env python
# pylint: disable=W0102, C0103, W0621, W0105, W0703
"""
_ProcessPool_

"""
from __future__ import print_function

from builtins import range, object

import zmq
import subprocess
import sys
import logging
import os
import threading
import traceback
import pickle

from Utils.PythonVersion import PY3

from logging.handlers import RotatingFileHandler

from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit

from WMCore.Services.Requests import JSONRequests

from WMCore.Agent.HeartbeatAPI import HeartbeatAPI

from WMCore.WMException import WMException


class ProcessPoolException(WMException):
    """
    _ProcessPoolException_

    Raise some exceptions
    """


class ProcessPoolWorker(object):
    """
    _ProcessPoolWorker_

    A basic worker object
    """

    def __init__(self, config):
        self.config = config

        return

    def __call__(self, input):
        """
        __call__

        input should be a single piece of work - a single list, a single
        dictionary, or a single variable of some sort
        """

        return


class ProcessPool(object):
    def __init__(self, slaveClassName, totalSlaves, componentDir,
                 config, namespace='WMComponent', inPort='5555',
                 outPort='5558'):
        """
        __init__

        Constructor for the process pool.  The slave class name must be based
        inside the WMComponent namespace.  For examples, the JobAccountant would
        pass in 'JobAccountant.AccountantWorker' to run the AccountantWorker
        class.  All log files will be stored in the component directory that is
        passed in.  Each slave will have its own log file.

        Note that the config is only used to determine database connection
        parameters.  It is not passed to the slave class.  The slaveInit
        parameter will be serialized and passed to the slave class's
        constructor.
        """
        self.enqueueIndex = 0
        self.dequeueIndex = 0
        self.runningWork = 0

        # Use the Services.Requests JSONizer, which handles __to_json__ calls
        self.jsonHandler = JSONRequests()

        # heartbeat should be registered at this point
        if getattr(config.Agent, "useHeartbeat", True):
            self.heartbeatAPI = HeartbeatAPI(getattr(config.Agent, "componentName", "ProcPoolSlave"))

        self.slaveClassName = slaveClassName
        self.componentDir = componentDir
        self.config = config
        self.versionString = "python3" if PY3 else "python2"

        self.workers = []
        self.nSlaves = totalSlaves
        self.namespace = namespace
        self.inPort = inPort
        self.outPort = outPort


        # Pickle the config
        self.configPath = os.path.join(componentDir, '%s_config.pkl' % slaveClassName)
        if os.path.exists(self.configPath):
            # Then we note it and overwrite it
            msg = "Something's in the way of the ProcessPool config: %s" % self.configPath
            logging.error(msg)
        with open(self.configPath, 'wb') as f:
            pickle.dump(config, f)

        # Set up ZMQ
        try:
            context = zmq.Context()
            self.sender = context.socket(zmq.PUSH)
            self.sender.bind("tcp://*:%s" % inPort)
            self.sink = context.socket(zmq.PULL)
            self.sink.bind("tcp://*:%s" % outPort)
        except zmq.ZMQError:
            # Try this again in a moment to see
            # if it's just being held by something pre-existing
            import time
            time.sleep(1)
            logging.error("Blocked socket on startup: Attempting sleep to give it time to clear.")
            try:
                context = zmq.Context()
                self.sender = context.socket(zmq.PUSH)
                self.sender.bind("tcp://*:%s" % inPort)
                self.sink = context.socket(zmq.PULL)
                self.sink.bind("tcp://*:%s" % outPort)
            except Exception as ex:
                msg = "Error attempting to open TCP sockets\n"
                msg += str(ex)
                logging.error(msg)
                import traceback
                print(traceback.format_exc())
                raise ProcessPoolException(msg)

        # Now actually create the slaves
        self.createSlaves()

        return

    def createSlaves(self):
        """
        _createSlaves_

        Create the slaves by using the values from __init__()
        Moving it into a separate function allows us to restart
        all of them.
        """

        totalSlaves = self.nSlaves
        slaveClassName = self.slaveClassName
        config = self.config
        namespace = self.namespace
        inPort = self.inPort
        outPort = self.outPort

        slaveArgs = [self.versionString, __file__, self.slaveClassName, inPort,
                     outPort, self.configPath, self.componentDir, self.namespace]

        count = 0
        while totalSlaves > 0:
            # For each worker you want create a slave process
            # That process calls this code (WMCore.ProcessPool) and opens
            # A process pool that loads the designated class
            slaveProcess = subprocess.Popen(slaveArgs, stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE)
            self.workers.append(slaveProcess)
            totalSlaves -= 1
            count += 1

        return

    def _subProcessName(self, slaveClassName, sequence):
        """ subProcessName for heartbeat
            could change to use process ID as a suffix
        """
        return "%s_%s" % (slaveClassName, sequence + 1)

    def __del__(self):
        """
        __del__

        Kill all the workers processes by sending them an invalid JSON object.
        This will cause them to shut down.
        """
        self.close()
        return

    def close(self):
        """
        _close_

        Close shuts down all the active systems by:

        a) Sending STOP commands for all workers
        b) Closing the pipes
        c) Shutting down the workers themselves
        """
        for i in range(self.nSlaves):
            try:
                encodedWork = self.jsonHandler.encode('STOP')
                if PY3:
                    self.sender.send_string(encodedWork)
                else:
                    self.sender.send(encodedWork)
            except Exception as ex:
                # Might be already failed.  Nothing you can
                # really do about that.
                logging.error("Failure killing running process: %s" % str(ex))
                pass

        try:
            self.sender.close()
        except:
            # We can't really do anything if we fail
            pass
        try:
            self.sink.close()
        except:
            # We can't do anything if we fail
            pass

        # Now close the workers by hand
        for worker in self.workers:
            try:
                worker.join()
            except Exception as ex:
                try:
                    worker.terminate()
                except Exception as ex2:
                    logging.error("Failure to join or terminate process")
                    logging.error(str(ex))
                    logging.error(str(ex2))
                    continue
        self.workers = []
        return

    def enqueue(self, work, list=False):
        """
        __enqeue__

        Assign work to the workers processes.  The work parameters must be a
        list where each item in the list can be serialized into JSON.

        If list is True, the entire list is sent as one piece of work
        """
        if len(self.workers) < 1:
            # Someone's shut down the system
            msg = "Attempting to send work after system failure and shutdown!\n"
            logging.error(msg)
            raise ProcessPoolException(msg)

        if not list:
            for w in work:
                encodedWork = self.jsonHandler.encode(w)
                if PY3:
                    self.sender.send_string(encodedWork)
                else:
                    self.sender.send(encodedWork)
                self.runningWork += 1
        else:
            encodedWork = self.jsonHandler.encode(work)
            if PY3:
                self.sender.send_string(encodedWork)
            else:
                self.sender.send(encodedWork)
            self.runningWork += 1

        return

    def dequeue(self, totalItems=1):
        """
        __dequeue__

        Retrieve completed work from the slave workers.  This method will block
        until enough work has been completed.
        """
        completedWork = []

        if totalItems > self.runningWork:
            msg = "Asked to dequeue more work then is running!\n"
            msg += "Failing"
            logging.error(msg)
            raise ProcessPoolException(msg)

        while totalItems > 0:
            try:
                output = self.sink.recv()
                decode = self.jsonHandler.decode(output)
                if isinstance(decode, dict) and decode.get('type', None) == 'ERROR':
                    # Then we had some kind of error
                    msg = decode.get('msg', 'Unknown Error in ProcessPool')
                    logging.error("Received Error Message from ProcessPool Slave")
                    logging.error(msg)
                    self.close()
                    raise ProcessPoolException(msg)
                completedWork.append(decode)
                self.runningWork -= 1
                totalItems -= 1
            except Exception as ex:
                msg = "Exception while getting slave output in ProcessPool.\n"
                msg += str(ex)
                logging.error(msg)
                break

        return completedWork

    def restart(self):
        """
        _restart_

        Delete everything and restart all pools
        """

        self.close()
        self.createSlaves()
        return


def setupLogging(componentDir):
    """
    _setupLogging_

    Setup logging for the slave process.  Each slave process will have its own
    log file.
    """
    logFile = "%s/ComponentLog.%s" % (componentDir, os.getpid())

    logHandler = RotatingFileHandler(logFile, "a", 1000000000, 3)
    logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    logHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(logHandler)
    logging.getLogger().setLevel(logging.INFO)
    # This is left in as a reminder for debugging purposes
    # SQLDEBUG turns your log files into horrible messes
    # logging.getLogger().setLevel(logging.SQLDEBUG)

    myThread = threading.currentThread()
    myThread.logger = logging.getLogger()
    return


def setupDB(config, wmInit):
    """
    _setupDB_

    Create the database connections.
    """
    socket = getattr(config.CoreDatabase, 'socket', None)
    connectUrl = config.CoreDatabase.connectUrl
    dialect = config.CoreDatabase.dialect

    wmInit.setDatabaseConnection(dbConfig=connectUrl,
                                 dialect=dialect,
                                 socketLoc=socket)
    return


if __name__ == "__main__":
    """
    __main__

    Entry point for the slave process.  The slave's classname will be passed in
    on the command line.  The database connection parameters as well as the
    name of the directory that the log files will be stored in will be passed
    in through stdin as a JSON object.

    Input variables:
    className, input port, output port, path to pickled config, component dir, namespace
    """

    # Get variables passed in
    slaveClassName = sys.argv[1]
    inPort = sys.argv[2]
    outPort = sys.argv[3]
    configPath = sys.argv[4]
    componentDir = sys.argv[5]
    namespace = sys.argv[6]

    # Set up logging
    setupLogging(componentDir)

    # Build ZMQ link
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:%s" % inPort)

    sender = context.socket(zmq.PUSH)
    sender.connect("tcp://localhost:%s" % outPort)

    # Build config
    if not os.path.exists(configPath):
        # We can do nothing -
        logging.error("Something in the way of the config path")
        sys.exit(1)

    with open(configPath, 'rb') as f:
        config = pickle.load(f)


    # Setup DB
    wmInit = WMInit()
    setupDB(config, wmInit)

    # Create JSON handler
    jsonHandler = JSONRequests()

    wmFactory = WMFactory(name="slaveFactory", namespace=namespace)
    slaveClass = wmFactory.loadObject(classname=slaveClassName, args=config)

    logging.info("Have slave class")

    while (True):
        encodedInput = receiver.recv()

        try:
            input = jsonHandler.decode(encodedInput)
        except Exception as ex:
            logging.error("Error decoding: %s" % str(ex))
            break

        if input == "STOP":
            break

        try:
            logging.error(input)
            output = slaveClass(input)
        except Exception as ex:
            crashMessage = "Slave process crashed with exception: " + str(ex)
            crashMessage += "\nStacktrace:\n"

            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
            for stackFrame in stackTrace:
                crashMessage += stackFrame

            logging.error(crashMessage)
            try:
                output = {'type': 'ERROR', 'msg': crashMessage}
                encodedOutput = jsonHandler.encode(output)
                if PY3:
                    sender.send_string(encodedOutput)
                else:
                    sender.send(encodedOutput)
                logging.error("Sent error message and now breaking")
                break
            except Exception as ex:
                logging.error("Failed to send error message")
                logging.error(str(ex))
                del jsonHandler
                sys.exit(1)

        if output != None:
            if isinstance(output, list):
                for item in output:
                    encodedOutput = jsonHandler.encode(item)
                    if PY3:
                        sender.send_string(encodedOutput)
                    else:
                        sender.send(encodedOutput)
            else:
                encodedOutput = jsonHandler.encode(output)
                if PY3:
                    sender.send_string(encodedOutput)
                else:
                    sender.send(encodedOutput)

    logging.info("Process with PID %s finished" % (os.getpid()))
    del jsonHandler
    sys.exit(0)
