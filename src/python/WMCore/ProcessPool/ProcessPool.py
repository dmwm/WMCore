#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, C0103, W0621, W0105, W0703
"""
_ProcessPool_

"""




import subprocess
import sys
import logging
import os
import threading
import traceback

from logging.handlers import RotatingFileHandler

from WMCore.WMFactory import WMFactory
from WMCore.WMInit    import WMInit
from WMCore           import WMLogging

from WMCore.Services.Requests import JSONRequests

from WMCore.Agent.HeartbeatAPI import HeartbeatAPI

class WorkerProcess:
    """
    Class for holding the subproc objects
    Makes it easier to do internal bookkeeping on
    how much work has been generated.
    """
    def __init__(self, subproc):
        """
        __init__
        
        This class just holds active ProcessPool subprocesses and does
        its own bookkeeping.
        """
        self.subproc   = subproc
        self.workCount = 0

        return

    def enqueue(self, work, length = 1):
        """
        _enqueue_
        
        Handle writing to the stdin of the subproc
        """
        self.subproc.stdin.write("%s\n" % work)
        self.subproc.stdin.flush()
        self.workCount += length
        return

    def dequeue(self):
        """
        _dequeue_
        
        Handle reading out of the subproc
        """
        if self.workCount == 0:
            # Then we have no work to return
            logging.error("Asked to return work for a thread that has no work assigned")
            #raise
            return

        output = self.subproc.stdout.readline()
        self.workCount -= 1

        return output

    def delete(self):
        """
        _delete_
        
        Delete the worker thread
        """
        self.subproc.stdin.write("\n")
        self.subproc.stdin.flush()

        return

    def runningWork(self):
        """
        _runningWork_
        
        Accessor method for the workCount
        """
        return self.workCount

class ProcessPool:
    def __init__(self, slaveClassName, totalSlaves, componentDir,
                 config, slaveInit = None, namespace = None):
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
        self.runningWork  = 0

        #Use the Services.Requests JSONizer, which handles __to_json__ calls
        self.jsonHandler = JSONRequests()
        
        # heartbeat should be registered at this point
        self.heartbeatAPI   = HeartbeatAPI(config.Agent.componentName)
        self.slaveClassName = slaveClassName
        self.componentDir   = componentDir
        self.config         = config
        # Grab the python version from the current version
        # Assume naming convention pythonA.B, i.e., python2.4 for v2.4.X
        majorVersion = sys.version_info[0]
        minorVersion = sys.version_info[1]

        if majorVersion and minorVersion:
            self.versionString = "python%i.%i" % (majorVersion, minorVersion)
        else:
            self.versionString = "python2.4"

        self.workers = []
        self.nSlaves = totalSlaves
        self.slaveInit = slaveInit
        self.namespace = namespace


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

        totalSlaves    = self.nSlaves
        slaveClassName = self.slaveClassName
        config         = self.config
        slaveInit      = self.slaveInit
        namespace      = self.namespace
        
        slaveArgs = [self.versionString, __file__, self.slaveClassName]
        if hasattr(config.CoreDatabase, "socket"):
            socket = config.CoreDatabase.socket
        else:
            socket = None

        (connectDialect, junk) = config.CoreDatabase.connectUrl.split(":", 1)
        if connectDialect.lower() == "mysql":
            dialect = "MySQL"
        elif connectDialect.lower() == "oracle":
            dialect = "Oracle"
        elif connectDialect.lower() == "sqlite":
            dialect = "SQLite"

        dbConfig = {"dialect": dialect,
                    "connectUrl": config.CoreDatabase.connectUrl,
                    "socket": socket,
                    "componentDir": self.componentDir}
        if namespace:
            # Then add a namespace to the config
            dbConfig['namespace'] = namespace
        encodedDBConfig = self.jsonHandler.encode(dbConfig)

        if slaveInit == None:
            encodedSlaveInit = None
        else:
            encodedSlaveInit = self.jsonHandler.encode(slaveInit)
        
        count = 0     
        while totalSlaves > 0:
            #For each worker you want create a slave process
            #That process calls this code (WMCore.ProcessPool) and opens
            #A process pool that loads the designated class
            slaveProcess = subprocess.Popen(slaveArgs, stdin = subprocess.PIPE,
                                            stdout = subprocess.PIPE)
            slaveProcess.stdin.write("%s\n" % encodedDBConfig)

            if encodedSlaveInit == None:
                slaveProcess.stdin.write("\n")
            else:
                slaveProcess.stdin.write("%s\n" % encodedSlaveInit)
                
            slaveProcess.stdin.flush()
            self.workers.append(WorkerProcess(subproc = slaveProcess))
            workerName = self._subProcessName(self.slaveClassName, count)
            
            self.heartbeatAPI.updateWorkerHeartbeat(workerName, pid = slaveProcess.pid)
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
        for worker in self.workers:
            try:
                worker.delete()
            except Exception, ex:
                pass

        self.workers = []

        return

    def enqueue(self, work):
        """
        __enqeue__

        Assign work to the workers processes.  The work parameters must be a
        list where each item in the list can be serialized into JSON.
        """

        workPerWorker = len(work) / len(self.workers)

        if workPerWorker == 0:
            workPerWorker = 1

        workIndex = 0
        while(len(work) > workIndex):
            workForWorker = work[workIndex : workIndex + workPerWorker]
            workIndex += workPerWorker
            length = len(workForWorker)
            

            encodedWork = self.jsonHandler.encode(workForWorker)

            worker = self.workers[self.enqueueIndex]
            worker.enqueue(work = encodedWork, length = length)
            
            self.heartbeatAPI.updateWorkerHeartbeat(
                            self._subProcessName(self.slaveClassName, 
                                                 self.enqueueIndex),
                            state = "Running")
            self.enqueueIndex = (self.enqueueIndex + 1) % len(self.workers)
            self.runningWork += length

        if len(work) > workIndex:
            encodedWork = self.jsonHandler.encode(work[workIndex:])
            length = len(work[workIndex:])

            worker = self.workers[self.enqueueIndex]
            self.enqueueIndex = (self.enqueueIndex + 1) % len(self.workers)
            worker.enqueue(work = encodedWork, length = length)
            self.runningWork += length
            
        return

    def dequeue(self, totalItems = 1):
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
            raise Exception(msg)

        while totalItems > 0:
            worker = self.workers[self.dequeueIndex]
            
            if not worker.runningWork() > 0:
                # Then the worker we've picked has no running work
                continue

            try:
                output = worker.dequeue()
                logging.error("dequeue: %s" % output)
                self.runningWork -= 1
                
                self.heartbeatAPI.updateWorkerHeartbeat(
                            self._subProcessName(self.slaveClassName, 
                                                 self.dequeueIndex),
                            state = "Done")

                if output == None:
                    logging.info("No output from worker node line in ProcessPool")
                    continue


                completedWork.append(self.jsonHandler.decode(output))
                totalItems -= 1
            except Exception, e:
                logging.error("Exception while getting slave output: %s" % e)
                break
            finally:
                self.dequeueIndex = (self.dequeueIndex + 1) % len(self.workers)

        return completedWork


    def restart(self):
        """
        _restart_

        Delete everything and restart all pools
        """

        self.__del__()
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
    #This is left in as a reminder for debugging purposes
    #SQLDEBUG turns your log files into horrible messes
    #logging.getLogger().setLevel(logging.SQLDEBUG)

    myThread = threading.currentThread()
    myThread.logger = logging.getLogger()
    return

def setupDB(config, wmInit):
    """
    _setupDB_

    Create the database connections.
    """
    if config.has_key("socket"):
        socket = config["socket"]
    else:
        socket = None
        
    connectUrl = config["connectUrl"]
    dialect = config["dialect"]
        
    wmInit.setDatabaseConnection(dbConfig = connectUrl,
                                 dialect = dialect,
                                 socketLoc = socket)
    return
    
if __name__ == "__main__":
    """
    __main__

    Entry point for the slave process.  The slave's classname will be passed in
    on the command line.  The database connection parameters as well as the
    name of the directory that the log files will be stored in will be passed
    in through stdin as a JSON object.
    """
    
    slaveClassName = sys.argv[1]

    jsonHandler = JSONRequests()

    encodedConfig = sys.stdin.readline()
    config = jsonHandler.decode(encodedConfig)

    encodedSlaveInit = sys.stdin.readline()
    if encodedSlaveInit != "\n":
        unicodeSlaveInit = jsonHandler.decode(encodedSlaveInit)
        slaveInit = {}
        for key in unicodeSlaveInit.keys():
            slaveInit[str(key)] = unicodeSlaveInit[key]
    else:
        slaveInit = None

    wmInit = WMInit()
    setupLogging(config["componentDir"])
    setupDB(config, wmInit)

    namespace = config.get('namespace', 'WMComponent')

    wmFactory = WMFactory(name = "slaveFactory", namespace = namespace)
    slaveClass = wmFactory.loadObject(classname = slaveClassName, args = slaveInit)

    logging.error("Have slave class")

    while(True):
        #Parameters for each job passed in from ProcessPool.enqueue()
        #Decoded by WMCore.Services.Requests class JSONRequests
        encodedInput = sys.stdin.readline()

        try:
            input = jsonHandler.decode(encodedInput)
        except Exception, ex:
            logging.error("Error decoding: %s" % str(ex))
            break

        #logging.info("Have parameters")
        #logging.info(input)

        try:
            output = slaveClass(parameters = input)
        except Exception, ex:
            crashMessage = "Slave process crashed with exception: " + str(ex)
            crashMessage += "\nStacktrace:\n"

            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
            for stackFrame in stackTrace:
                crashMessage += stackFrame
                
            logging.error(crashMessage)
            sys.exit(1)
            
        if output != None:
            if type(output) == list:
                for item in output:
                    encodedOutput = jsonHandler.encode(item)
                    sys.stdout.write("%s\n" % encodedOutput)
                    sys.stdout.flush()
            else:
                encodedOutput = jsonHandler.encode(output)
                sys.stdout.write("%s\n" % encodedOutput)
                sys.stdout.flush()

    logging.info("Process with PID %s finished" %(os.getpid()))
    sys.exit(0)
