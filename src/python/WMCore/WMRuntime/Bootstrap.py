#!/usr/bin/env python
"""
_TaskSpace_

Frontend module for setting up TaskSpace & StepSpace areas within a job.

"""
import inspect
import pickle
import os
import os.path
import logging
import threading
import sys
from logging.handlers import RotatingFileHandler

from WMCore.WMException        import WMException
from WMCore.WMRuntime          import TaskSpace
from WMCore.WMRuntime          import StepSpace
from WMCore                    import WMLogging
from WMCore.WMRuntime.Watchdog import Watchdog

from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper



class BootstrapException(WMException):
    #TODO: make awesome
    pass



def establishTaskSpace(**args):
    """
    _establishTaskSpace_

    Bootstrap method for the execution dir for a WMTask

    """
    return TaskSpace.TaskSpace(**args)

def establishStepSpace(**args):
    """
    _establishStepSpace_

    Bootstrap method for the execution dir of a WMStep within a WMTask

    """
    return StepSpace.StepSpace(**args)

def locateWMSandbox():
    """
    _locateWMSandbox_

    At runtime, the WMSandbox module should be defined and available
    on the PYTHONPATH.

    Look up the location of the module via import so that the
    pickled files in the sandbox can be loaded

    """
    try:
        import WMSandbox
    except ImportError, ex:
        msg = "Error importing WMSandbox module"
        msg += str(ex)
        raise BootstrapException(msg)

    wmsandboxLoc = inspect.getsourcefile(WMSandbox)
    print "Have sourcefile location"
    print WMSandbox
    print wmsandboxLoc
    print inspect.getmoduleinfo(wmsandboxLoc)
    wmsandboxLoc = wmsandboxLoc.replace("__init__.py", "")
    return wmsandboxLoc

def loadJobDefinition():
    """
    _loadJobDefinition_

    Load the job package and pull out the indexed job, return
    WMBS Job instance

    """
    sandboxLoc = locateWMSandbox()
    package = JobPackage()
    packageLoc = os.path.join(sandboxLoc, "JobPackage.pcl")
    try:
        package.load(packageLoc)
    except Exception, ex:
        msg = "Failed to load JobPackage:%s\n" % packageLoc
        msg += str(ex)
        raise BootstrapException, msg

    try:
        import WMSandbox.JobIndex
    except ImportError, ex:
        msg = "Failed to import WMSandbox.JobIndex module\n"
        msg += str(ex)
        raise BootstrapException, msg

    index = WMSandbox.JobIndex.jobIndex

    try:
        job = package[index]
    except Exception, ex:
        msg = "Failed to extract Job %i\n" %(index)
        msg += str(ex)
        raise BootstrapException, msg
    diagnostic = """
    Job Index = %s
    Job Instance = %s
    """ % (index, job)
    logging.info(diagnostic)

    return job

def loadWorkload():
    """
    _loadWorkload_

    Load the Workload from the WMSandbox Area

    """
    sandboxLoc = locateWMSandbox()
    workloadPcl = "%s/WMWorkload.pkl" % sandboxLoc
    handle = open(workloadPcl, 'r')
    wmWorkload = pickle.load(handle)
    handle.close()

    return WMWorkloadHelper(wmWorkload)




def loadTask(job):
    """
    _loadTask_

    load the Workload, and then lookup the task in the workload
    required by the job

    """
    workload = loadWorkload()

    try:
        task = workload.getTaskByPath(job['task'])
    except Exception, ex:
        msg = "Error looking up task %s\n" % job['task']
        msg += str(ex)
        raise BootstrapException, msg
    if task == None:
        msg = "Unable to look up task %s from Workload\n" % job['task']
        msg += "Task name not matched"
        raise BootstrapException, msg
    return task



def setupLogging(logDir):
    """
    _setupLogging_

    Setup logging for the slave process.  Each slave process will have its own
    log file.
    """
    try:
        logFile = "%s/jobLog.%s.log" % (logDir, os.getpid())
        
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
    except Exception, ex:
        msg = "Error setting up logging in dir %s:\n" % logDir
        msg += str(ex)
        raise BootstrapException, msg        
    return


def setupMonitoring():
    try:
        monitor = Watchdog()
        myThread = threading.currentThread
        myThread.watchdogMonitor = monitor
        return monitor
    except Exception, ex:
        msg = "Error setting up Watchdog monitoring:\n"
        msg += str(ex)
        raise BootstrapException, msg       

