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
import socket
from logging.handlers import RotatingFileHandler

from WMCore.WMException        import WMException
from WMCore.WMRuntime          import TaskSpace
from WMCore.WMRuntime          import StepSpace
from WMCore                    import WMLogging
from WMCore.WMRuntime.Watchdog import Watchdog

from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.WMSpec.WMWorkload      import WMWorkloadHelper

from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, SiteConfigError
import WMCore.FwkJobReport.Report as Report


class BootstrapException(WMException):
    #TODO: make awesome
    pass


# Copied direct from ProdAgent to find the damn CE name
def getSyncCE():
    """
    _getSyncCE_

    Extract the SyncCE from GLOBUS_GRAM_JOB_CONTACT if available for OSG,
    otherwise broker info for LCG

    """
    result = socket.gethostname()

    if os.environ.has_key('GLOBUS_GRAM_JOB_CONTACT'):
        #  //
        # // OSG, Sync CE from Globus ID
        #//
        val = os.environ['GLOBUS_GRAM_JOB_CONTACT']
        try:
            host = val.split("https://", 1)[1]
            host = host.split(":")[0]
            result = host
        except:
            pass
        return result
    if os.environ.has_key('EDG_WL_JOBID'):
        #  //
        # // LCG, Sync CE from edg command
        #//
        command = "glite-brokerinfo getCE"
        pop = popen2.Popen3(command)
        pop.wait()
        exitCode = pop.poll()
        if exitCode:
            return result 
        
        content = pop.fromchild.read()
        result = content.strip()
        return result

    if os.environ.has_key('NORDUGRID_CE'):
        #  //
        # // ARC, Sync CE from env. var. submitted with the job by JobSubmitter
        #//
        return os.environ['NORDUGRID_CE']

    return result




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
        createErrorReport(exitCode = 11001, errorType = "JobPackageError", errorDetails = msg)
        raise BootstrapException, msg

    try:
        import WMSandbox.JobIndex
    except ImportError, ex:
        msg = "Failed to import WMSandbox.JobIndex module\n"
        msg += str(ex)
        createErrorReport(exitCode = 11002, errorType = "JobIndexError", errorDetails = msg)
        raise BootstrapException, msg

    index = WMSandbox.JobIndex.jobIndex

    try:
        job = package[index]
    except Exception, ex:
        msg = "Failed to extract Job %i\n" % (index)
        msg += str(ex)
        createErrorReport(exitCode = 11003, errorType = "JobExtractionError", errorDetails = msg)
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
    except KeyError, ex:
        msg =  "Task name not in job object"
        msg += str(ex)
        createErrorReport(exitCode = 11103, errorType = "TaskNotInJob", errorDetails = msg)
        raise BootstrapException, msg
    except Exception, ex:
        msg = "Error looking up task %s\n" % job['task']
        msg += str(ex)
        createErrorReport(exitCode = 11101, errorType = "TaskLookupError", errorDetails = msg)
        raise BootstrapException, msg
    if task == None:
        msg = "Unable to look up task %s from Workload\n" % job['task']
        msg += "Task name not matched"
        createErrorReport(exitCode = 11102, errorType = "TaskNotFound", errorDetails = msg)
        raise BootstrapException, msg
    return task


def createInitialReport(job, task, logLocation):
    """
    _createInitialReport_

    Create an initial job report with the base
    information in it.
    """
    try:
        siteCfg = loadSiteLocalConfig()
    except SiteConfigError:
        # For now, assume that we did this on purpose
        msg = "Couldn't find SiteConfig"
        logging.error(msg)
    report  = Report.Report()


    report.data.WMAgentJobID   = job.get('id', None)
    report.data.WMAgentJobName = job.get('name', None)
    report.data.seName         = siteCfg.localStageOut.get('se-name',
                                                           socket.gethostname())
    report.data.siteName       = getattr(siteCfg, 'siteName', 'Unknown')
    report.data.hostName       = socket.gethostname()
    report.data.ceName         = getSyncCE()
    report.data.completed      = False
    report.setTaskName(taskName = job.get('task', 'TaskNotFound'))

    # Not so fond of this, but we have to put the master
    # report way up at the top so it's returned if the
    # job fails early
    reportPath = os.path.join(os.getcwd(), '../', logLocation)
    report.save(reportPath)

    return



def createErrorReport(exitCode, errorType, errorDetails = None,
                      logLocation = "Report.pkl"):
    """
    _createErrorReport_

    Create a report if something fails inside the Bootstrap
    This creates a dummy step called 'CRITICAL' and
    sticks the error in there.
    """


    try:
        siteCfg = loadSiteLocalConfig()
    except SiteConfigError:
        # For now, assume that we did this on purpose
        msg = "Couldn't find SiteConfig"
        logging.error(msg)
    report  = Report.Report()


    report.data.seName         = siteCfg.localStageOut.get('se-name',
                                                           socket.gethostname())
    report.data.siteName       = getattr(siteCfg, 'siteName', 'Unknown')
    report.data.hostName       = socket.gethostname()
    report.data.ceName         = getSyncCE()
    report.data.completed      = False


    report.addError(stepName = 'CRITICAL', exitCode = exitCode,
                    errorType = errorType, errorDetails = errorDetails)

    reportPath = os.path.join(os.getcwd(), '../', logLocation)
    report.save(reportPath)

    return

    



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
    """
    Setup the basics of the watchdog monitoring.
    Attach it to a thread.

    """
    try:
        monitor = Watchdog()
        myThread = threading.currentThread
        myThread.watchdogMonitor = monitor
        return monitor
    except Exception, ex:
        msg = "Error setting up Watchdog monitoring:\n"
        msg += str(ex)
        raise BootstrapException, msg       

