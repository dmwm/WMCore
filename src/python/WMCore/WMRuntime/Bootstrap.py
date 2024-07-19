#!/usr/bin/env python
"""
_TaskSpace_

Frontend module for setting up TaskSpace & StepSpace areas within a job.
"""

import inspect
import logging
import os
import os.path
import pickle
import socket
import sys
import threading
import json
from logging.handlers import RotatingFileHandler

import WMCore.FwkJobReport.Report as Report
from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, SiteConfigError
from WMCore.WMException import WMException
from WMCore.WMRuntime import StepSpace
from WMCore.WMRuntime import TaskSpace
from WMCore.WMRuntime.Watchdog import Watchdog
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMRuntime.Tools.Scram import getPlatformMachine
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin

class BootstrapException(WMException):
    """ An awesome exception """
    pass


def readFloatFromFile(filePath):
    try:
        with open(filePath, "r") as nf:
            return float(nf.readline())
    except:
        return None


# Copied direct from ProdAgent to find the damn CE name
def getSyncCE():
    """
    _getSyncCE_

    Extract the SyncCE from GLOBUS_GRAM_JOB_CONTACT if available for OSG,
    otherwise broker info for LCG

    """
    result = socket.gethostname()

    if 'GLOBUS_GRAM_JOB_CONTACT' in os.environ:
        #  //
        # // OSG, Sync CE from Globus ID
        # //
        val = os.environ['GLOBUS_GRAM_JOB_CONTACT']
        try:
            host = val.split("https://", 1)[1]
            host = host.split(":")[0]
            result = host
        except Exception:
            logging.warning("Failed to extract SyncCE from globus")
        return result

    if 'NORDUGRID_CE' in os.environ:
        #  //
        # // ARC, Sync CE from env. var. submitted with the job by JobSubmitter
        # //
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
    except ImportError as ex:
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

    Although this will create a JobReport, it won't necessarily bring it back.
    Report names are dependent on the retry_count, but if it fails unpacking the job
    it doesn't know the retry_count and will create the wrong file
    """
    sandboxLoc = locateWMSandbox()
    package = JobPackage()
    packageLoc = os.path.join(sandboxLoc, "JobPackage.pcl")
    try:
        package.load(packageLoc)
    except Exception as ex:
        msg = "Failed to load JobPackage:%s\n" % packageLoc
        msg += str(ex)
        createErrorReport(exitCode=11001, errorType="JobPackageError", errorDetails=msg)
        raise BootstrapException(msg)

    try:
        import WMSandbox.JobIndex
    except ImportError as ex:
        msg = "Failed to import WMSandbox.JobIndex module\n"
        msg += str(ex)
        createErrorReport(exitCode=11002, errorType="JobIndexError", errorDetails=msg)
        raise BootstrapException(msg)

    index = WMSandbox.JobIndex.jobIndex

    try:
        job = package[index]
    except Exception:
        msg = "Failed to extract job index %i " % index
        msg += "from the jobPackage directory: %s\n" % package.get('directory')
        msg += "Found a total of %d indexes in the JobPackage.\n" % len(package)
        createErrorReport(exitCode=11003, errorType="JobExtractionError", errorDetails=msg)
        raise BootstrapException(msg)
    logging.info("Job Index = %s\nJob Instance = %s\n", index, job)

    return job


def loadWorkload():
    """
    _loadWorkload_

    Load the Workload from the WMSandbox Area

    """
    sandboxLoc = locateWMSandbox()
    workloadPcl = "%s/WMWorkload.pkl" % sandboxLoc
    with open(workloadPcl, 'rb') as handle:
        wmWorkload = pickle.load(handle)

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
    except KeyError as ex:
        msg = "Task name not in job object"
        msg += str(ex)
        createErrorReport(exitCode=11103, errorType="TaskNotInJob", errorDetails=msg,
                          logLocation="Report.%i.pkl" % job['retry_count'])
        raise BootstrapException(msg)
    except Exception as ex:
        msg = "Error looking up task %s\n" % job['task']
        msg += str(ex)
        createErrorReport(exitCode=11101, errorType="TaskLookupError", errorDetails=msg,
                          logLocation="Report.%i.pkl" % job['retry_count'])
        raise BootstrapException(msg)
    if task is None:
        msg = "Unable to look up task %s from Workload\n" % job['task']
        msg += "Task name not matched"
        createErrorReport(exitCode=11102, errorType="TaskNotFound", errorDetails=msg,
                          logLocation="Report.%i.pkl" % job['retry_count'])
        raise BootstrapException(msg)
    return task

def createWMRuntimeJson(outputPath, jobTypes=("Production", "Processing")):
    """
    Create a json with runtime information in the following form below:
    {
     "workflow_type: "Stepchain",  # string with the request type
     "number_of_cmsRuns": 2,  # an integer >= 0
     "worker_arch": "X86_64",  # string with the architecture of the worker node
     "worker_os": "rhel9",  # string with the OS of the worker node
     "job_type": "Production", # string with job type information: Production/Processing, Merge, LogCollect, etc.
     "cmsRun_params": [{
         "step": "cmsRun1",  # string with the relevant step
         "keek_output": boolean saying whether we keep output or not,
         "input_files": [string of input LFNs or a single local file],
         "output_files": [ordered string of output local files],
         "output_files_datatiers": [ordered string of output local files datatiers],
         "transient_output": [ordered boolean saying whether output files are announced in the workflow or not],
         "output_files_unmerged_base": [ordered string with lfnBase],
         "output_files_unmerged_base": [ordered string with mergedLFNBase info]
         },
         {
         "step": "cmsRun2",  # string with the relevant step
         # and so on...
         },
         # any new step appends step data to this list
     ]
     } 
     :param outputPath: path to write the json
     :param jobTypes: tuple of job types to send to logging. E.g.: ("Production", "Processing", "Merge", "LogCollect")
     :return
    """
    # Load workflow-level information
    workload = loadWorkload()
    job = loadJobDefinition()
    task = loadTask(job)
    # Create dictionary to collect runtime info
    runtimeInfo = {}
    runtimeInfo['workflow_type'] = workload.getRequestType()
    runtimeInfo['number_of_cmsRuns'] = len(task.listAllStepNames(cmsRunOnly=True))
    runtimeInfo['worker_arch'] = getPlatformMachine()
    runtimeInfo['worker_os'] = BasePlugin.scramArchtoRequiredOS(task.getScramArch())
    runtimeInfo['job_type'] = job.get('jobType', None)
    cmsRun_params = []
    # Jobs with MCFakeFile means no input files
    mcFakeFile=False
    if len(job['input_files']) == 1:
        if job['input_files'][0]['lfn'].startswith("MCFakeFile"):
            mcFakeFile=True
    # Get information per cmsRun step
    for cmsswStep in task.listAllStepNames(cmsRunOnly=True):
        cmsRunParam = {}
        step = task.getStepHelper(cmsswStep)
        output_modules = step.listOutputModules()
        keepOutput = getattr(step.data.output, 'keep')
        # Collect input files
        inputFileNames = []
        if mcFakeFile:
            # For chained processes, point to previous step output
            chained = getattr(step.data.input, 'chainedProcessing', False)
            if chained:
                inputModule = getattr(step.data.input, 'inputOutputModule', None)
                inputStepName = getattr(step.data.input, 'inputStepName', None)
                inputFileNames.append("../{}/{}.root".format(inputStepName, inputModule))
        else:
            # Get input files from job package information
            for inputFiles in job.get('input_files', []):
                inputFileNames.append(inputFiles['lfn'])

        # Collect output files per module
        outputFileNames = []
        outputFilesDataTiers = []
        outputFilesTransient = []
        outputFilesLFNBase = []
        outputFilesMergedLFNBase = []
        for moduleName in output_modules:
            outputModule = step.getOutputModule(moduleName)
            output_filename = "{}.root".format(moduleName)
            outputFileNames.append(output_filename)
            outputFilesDataTiers.append(getattr(outputModule, 'dataTier', None))
            outputFilesTransient.append(getattr(outputModule, 'transient', None))
            outputFilesLFNBase.append(getattr(outputModule, 'lfnBase', None))
            outputFilesMergedLFNBase.append(getattr(outputModule, 'mergedLFNBase', None))

        cmsRunParam['step'] = step.stepName()
        cmsRunParam['keep_output'] = keepOutput
        cmsRunParam['input_files'] = inputFileNames
        cmsRunParam['output_files'] = outputFileNames
        cmsRunParam['output_files_datatiers'] = outputFilesDataTiers
        cmsRunParam['transient_output'] = outputFilesTransient
        cmsRunParam['output_files_unmerged_base'] = outputFilesLFNBase
        cmsRunParam['output_files_merged_base'] = outputFilesMergedLFNBase
        cmsRun_params.append(cmsRunParam)
    runtimeInfo['cmsRun_params'] = cmsRun_params

    jsonPath = "{}/runtimeInfo.json".format(outputPath)
    with open(jsonPath, "w", encoding="utf-8") as f:
        json.dump(runtimeInfo, f, indent=4, sort_keys=True)

    # Log Production/Processing
    if runtimeInfo['job_type'] in jobTypes:
        logging.info('runtime json = {}'.format(json.dumps(runtimeInfo, indent=4, sort_keys=True)))

    return

def createInitialReport(job, reportName):
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
        # TODO: Make less goatballs for testing purposes
        return

    report = Report.Report()

    report.data.WMAgentJobID = job.get('id', None)
    report.data.WMAgentJobName = job.get('name', None)
    report.data.pnn = siteCfg.localStageOut.get('phedex-node', 'Unknown')
    report.data.siteName = getattr(siteCfg, 'siteName', 'Unknown')
    report.data.hostName = socket.gethostname()
    report.data.ceName = getSyncCE()

    # TODO: need to check what format it returns and what features need to extract.
    # currently
    # $MACHINEFEATURES/hs06: HS06 score of the host
    # $MACHINEFEATURES/total_cpu: number of configured job slots
    # $JOBFEATURES/hs06_job: HS06 score available to your job
    # $JOBFEATURES/allocated_cpu: number of allocated slots (=8 in case of a multicore job

    machineFeaturesFile = os.environ.get('MACHINEFEATURES')
    report.data.machineFeatures = {}
    if machineFeaturesFile:
        report.data.machineFeatures['hs06'] = readFloatFromFile("%s/hs06" % machineFeaturesFile)
        report.data.machineFeatures['total_cpu'] = readFloatFromFile("%s/total_cpu" % machineFeaturesFile)

    jobFeaturesFile = os.environ.get('JOBFEATURES')
    report.data.jobFeatures = {}
    if jobFeaturesFile:
        report.data.jobFeatures['hs06_job'] = readFloatFromFile("%s/hs06_job" % jobFeaturesFile)
        report.data.jobFeatures['allocated_cpu'] = readFloatFromFile("%s/allocated_cpu" % jobFeaturesFile)

    report.data.completed = False
    report.setTaskName(taskName=job.get('task', 'TaskNotFound'))

    # Not so fond of this, but we have to put the master
    # report way up at the top so it's returned if the
    # job fails early
    reportPath = os.path.join(os.getcwd(), '../', reportName)
    report.save(reportPath)

    return


def createErrorReport(exitCode, errorType, errorDetails=None,
                      logLocation="Report.0.pkl"):
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
        # TODO: Make this not suck goatballs when you are just running tests
        return
    report = Report.Report()

    report.data.pnn = siteCfg.localStageOut.get('phedex-node', 'Unknown')
    report.data.siteName = getattr(siteCfg, 'siteName', 'Unknown')
    report.data.hostName = socket.gethostname()
    report.data.ceName = getSyncCE()
    report.data.completed = False

    report.addError(stepName='CRITICAL', exitCode=exitCode,
                    errorType=errorType, errorDetails=errorDetails)

    reportPath = os.path.join(os.getcwd(), '../', logLocation)
    report.save(reportPath)

    return


def setupLogging(logDir, logName="wmagentJob.log", useStdout=False):
    """
    _setupLogging_

    Setup logging for the slave process. Each slave process will have its own
    log file.
    useStdout adds a stdout handler to the logger object such that records are
    also written to the main job log object.
    """
    try:
        # create a root logger
        logger = logging.getLogger()
        # create a log formatter
        logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")

        if useStdout:
            consoleHandler = logging.StreamHandler(sys.stdout)
            consoleHandler.setFormatter(logFormatter)
            logger.addHandler(consoleHandler)
        else:
            # create a file handler
            logFile = "%s/%s" % (logDir, logName)
            logHandler = RotatingFileHandler(logFile, "a", 1000000000, 3)
            logHandler.setFormatter(logFormatter)
            logger.addHandler(logHandler)

        logger.setLevel(logging.INFO)

        # This is left in as a reminder for debugging purposes
        # SQLDEBUG turns your log files into horrible messes
        # logging.getLogger().setLevel(logging.SQLDEBUG)
        myThread = threading.currentThread()
        myThread.logger = logger
    except Exception as ex:
        msg = "Error setting up logging in dir %s:\n" % logDir
        msg += str(ex)
        raise BootstrapException(msg)
    return


def setupMonitoring(logName):
    """
    Setup the basics of the watchdog monitoring.
    Attach it to a thread.

    """
    try:
        monitor = Watchdog(logPath=logName)
        myThread = threading.currentThread
        myThread.watchdogMonitor = monitor
        return monitor
    except Exception as ex:
        msg = "Error setting up Watchdog monitoring:\n"
        msg += str(ex)
        raise BootstrapException(msg)
