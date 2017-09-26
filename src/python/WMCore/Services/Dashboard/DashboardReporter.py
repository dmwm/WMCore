#!/usr/bin/env python
"""
__DashboardReporter__
Utilities for reporting information to the dashboard
"""

import time
import logging

from WMCore import __version__
from WMCore.WMException                     import WMException
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Services.Dashboard.DashboardAPI import DashboardAPI


# xdrlib cannot handle unicode strings
def unicodeToStr(value):
    if not isinstance(value, basestring):
        return 'unknown'
    try:
        return str(value)
    except UnicodeEncodeError:
        #This contains some unicode outside ascii range
        return 'unknown'


class DashboardReporterException(WMException):
    """
    _DashboardReporterException_

    Something's wrong when pushing the information out.
    """

    pass


class DashboardReporter(WMObject):
    """
    _DashboardReporter_

    Reports job/task information in the dashboard
    """

    def __init__(self, config):
        self.config = config

        #Have to default this to the local host otherwise a lot of unit tests die
        if hasattr(config, 'DashboardReporter'):
            self.destHost = getattr(self.config.DashboardReporter, 'dashboardHost', '127.0.0.1')
            self.destPort = getattr(self.config.DashboardReporter, 'dashboardPort', 8884)
        else:
            self.destHost = '127.0.0.1'
            self.destPort = 8884

        self.taskPrefix = 'wmagent_'
        self.tsFormat = '%Y-%m-%d %H:%M:%S'

        self.dashboardUrl = '%s:%s' % (self.destHost, str(self.destPort))

    def handleCreated(self, jobs):
        """
        _handleCreated_

        Handle the created jobs:
        Publish the jobs' meta information (and tasks' if not in the Cache) to
        the dashboard
        jobs must be a list of dictionaries with the following information
        (* denotes an optional argument):
            workflow -> Name of the workflow
            name -> unique name of the job
            retry_count -> retry count of the job
            taskType -> Workflow type (production, etc...)
            jobType -> Job type (merge, processing, etc...)
            *NEventsToprocess -> Number of events the job will process
        Additionally the job should carry information about the task according
        to the description of the addTask method
        """
        logging.info ("Handling %d created jobs" % len(jobs))

        jobParams = []
        for job in jobs:
            logging.debug("Sending info for job %s" % str(job))

            jobid = '%s_%i' % (job['name'], job['retry_count'])
            package = {}
            package['MessageType'] = 'JobMeta'
            package['jobId'] = unicodeToStr(jobid)
            package['taskId'] = unicodeToStr(self.taskPrefix + job['workflow'])
            package['TaskType'] = job['taskType']
            package['JobType'] = job['jobType']
            package['NEventsToProcess'] = job.get('nEventsToProc', 'NotAvailable')
            jobParams.append(package)

        with DashboardAPI(logr=logging, server=self.dashboardUrl) as dashboard:
            dashboard.apMonSend(jobParams)

        return

    def handleJobStatusChange(self, jobs, statusValue, statusMessage):
        """
        _handleJobStatusChange_

        Handle the submitted, completed or killed jobs:
        Publish the status information to the dashboard
        jobs must be a list of dictionaries with the following information
        (* denotes an optional argument):
            workflow -> Name of the workflow
            name -> unique name of the job
            retry_count -> retry count of the job
            statusValue -> Job status value
            statusMessage -> Message from the new status
            *location -> Computing element the job is destinated to
            *fwjr -> Post processing step information
        """
        logging.info("Handling %d jobs" % len(jobs))

        jobParams = []
        for job in jobs:
            logging.debug("Sending info for job %s" % str(job))

            jobid = '%s_%i' % (job['name'], job['retry_count'])
            package = {}
            package['MessageType'] = 'JobStatus'
            package['jobId'] = unicodeToStr(jobid)
            package['taskId'] = unicodeToStr(self.taskPrefix + job['workflow'])
            package['StatusValue'] = statusValue
            package['StatusValueReason'] = statusMessage
            package['StatusEnterTime'] = time.strftime(self.tsFormat, time.gmtime())
            package['StatusDestination'] = job.get('location', 'NotAvailable')
            if job.get('plugin', None):
                package['scheduler'] = job['plugin'][:-6]
            jobParams.append(package)

        with DashboardAPI(logr=logging, server=self.dashboardUrl) as dashboard:
            dashboard.apMonSend(jobParams)

        # send step information, if available
        self.handleSteps(jobs)

        return

    def handleSteps(self, jobs):
        """
        _handleSteps_

        Handle the post-processing step information
        """
        if not isinstance(jobs, list):
            jobs = [jobs]

        jobParams = []
        for job in jobs:
            if job['fwjr'] is None:
                return

            steps = job['fwjr'].listSteps()
            for stepName in steps:
                step = job['fwjr'].retrieveStep(stepName)
                if not hasattr(step, 'counter'):
                    continue

                counter = step.counter

                package = {}
                package.update(self.getPerformanceInformation(step))
                package.update(self.getEventInformation(stepName, job['fwjr']))

                # Input files should just be appended onto inputFiles instead of given a step #
                # per https://hypernews.cern.ch/HyperNews/CMS/get/comp-monitoring/326.html
                inputFilePackage = self.getInputFilesInformation(step)
                if inputFilePackage:
                    if 'inputFiles' in package:
                        package['inputFiles'] += ';' +  inputFilePackage['inputFiles']
                    else:
                        package.update(self.getInputFilesInformation(step))

                trimmedPackage = {}
                for key in package:
                    if key in ['inputFiles', 'Basename', 'inputBlocks']:
                        trimmedPackage[key] = package[key]
                    elif package[key] is not None:
                        trimmedPackage['%d_%s' % (counter, key)] = package[key]
                package = trimmedPackage

                if not package:
                    continue

                jobid = '%s_%i' % (job['name'], job['retry_count'])
                package['jobId'] = unicodeToStr(jobid)
                package['taskId'] = unicodeToStr(self.taskPrefix + job['workflow'])
                package['%d_stepName' % counter] = stepName

                logging.debug("Sending step info: %s" % str(package))
                jobParams.append(package)

        with DashboardAPI(logr=logging, server=self.dashboardUrl) as dashboard:
            dashboard.apMonSend(jobParams)

        return

    def getEventInformation(self, stepName, fwjr):
        """
        _getEventInformation_

        Handles the information about input and output files in the step
        and provides detailed event information to be sent to the dashboard

        """
        inputEvents = 0
        inputFiles = fwjr.getInputFilesFromStep(stepName = stepName)
        for inputFile in inputFiles:
            inputEvents += inputFile['events']

        outputEventInfo = ''
        step = fwjr.retrieveStep(stepName)
        outputModules = getattr(step, 'outputModules', None)

        if outputModules:
            for outputMod in outputModules:

                # we don't report logArchive, LogCollect and Sqlite output
                if outputMod in [ "logArchive", "LogCollect", "SqliteALCAPROMPT" ]:
                    continue

                outFiles = fwjr.getFilesFromOutputModule(step = stepName,
                                                         outputModule = outputMod)

                # an output module can write multiple files, but
                # they all have the same dataset and data tier
                events = 0
                dataTier = None
                procDataset = None
                for outFile in outFiles:

                    dataTier = outFile['dataset'].get('dataTier', None)
                    procDataset = outFile['dataset'].get('processedDataset', None)
                    if dataTier and procDataset:
                        events += outFile['events']
                    else:
                        logging.error('Output module %s has a file %s with incomplete info'
                                      % (outputMod, outFile['lfn']))

                if dataTier and procDataset:
                    outputEventInfo += '%s:%s:%d;' % (procDataset, dataTier, events)

        # take off the last ;
        if outputEventInfo:
            outputEventInfo = outputEventInfo[:-1]

        if inputEvents or outputEventInfo:
            return { 'inputEvents' : inputEvents,
                     'OutputEventInfo' : outputEventInfo }
        else:
            return {}

    def getPerformanceInformation(self, step):
        """
        _getPerformanceInformation_

        Handles the performance information about a step and builds a dict
        to send to the dashboard
        """
        performance = step.performance

        if not hasattr(performance, 'memory'):
            performance.section_('memory')
        if not hasattr(performance, 'storage'):
            performance.section_('storage')
        if not hasattr(performance, 'cpu'):
            performance.section_('cpu')

        package = {}

        package['PeakValueRss']           = getattr(performance.memory,
                                                    'PeakValueRss', None)
        package['PeakValuePss']           = getattr(performance.memory,
                                                    'PeakValuePss', None)
        package['PeakValueVsize']           = getattr(performance.memory,
                                                    'PeakValueVsize', None)
        package['writeTotalMB']           = getattr(performance.storage,
                                                    'writeTotalMB', None)
        package['readPercentageOps']      = getattr(performance.storage,
                                                    'readPercentageOps',
                                                    None)
        package['readAveragekB']          = getattr(performance.storage,
                                                    'readAveragekB', None)
        package['readTotalMB']            = getattr(performance.storage,
                                                    'readTotalMB', None)
        package['readNumOps']             = getattr(performance.storage,
                                                    'readNumOps', None)
        package['readCachePercentageOps'] = getattr(performance.storage,
                                                    'readCachePercentageOps'
                                                    , None)
        package['readMBSec']              = getattr(performance.storage,
                                                    'readMBSec', None)
        package['readMaxMSec']            = getattr(performance.storage,
                                                    'readMaxMSec', None)
        package['readTotalSecs']          = getattr(performance.storage,
                                                    'readTotalSecs', None)
        package['writeTotalSecs']         = getattr(performance.storage,
                                                    'writeTotalSecs', None)
        package['TotalJobCPU']            = getattr(performance.cpu,
                                                    'TotalJobCPU', None)
        package['AvgEventCPU']            = getattr(performance.cpu,
                                                    'AvgEventCPU', None)
        package['MaxEventTime']           = getattr(performance.cpu,
                                                    'MaxEventTime', None)
        package['AvgEventTime']           = getattr(performance.cpu,
                                                    'AvgEventTime', None)
        package['MinEventCPU']            = getattr(performance.cpu,
                                                    'MinEventCPU', None)
        package['TotalEventCPU']          = getattr(performance.cpu,
                                                    'TotalEventCPU', None)
        package['TotalJobTime']           = getattr(performance.cpu,
                                                    'TotalJobTime', None)
        package['MinEventTime']           = getattr(performance.cpu,
                                                    'MinEventTime', None)
        package['MaxEventCPU']            = getattr(performance.cpu,
                                                    'MaxEventCPU', None)

        return package

    def getInputFilesInformation(self, step):
        """
        Determines the input files and parent input files and
        if they were read correctly, skipped, or read through fallback
        """

        files = {}
        package = {}

        try:
            if hasattr(step, 'input') and hasattr(step.input, 'source'):
                for fileobj in step.input.source.files:
                    if hasattr(fileobj, 'lfn'):
                        lfn = fileobj.lfn
                        inputType = getattr(fileobj, 'input_type', 'primaryFiles')
                        files.update({lfn: {'status': 'Local', 'type': inputType}})

            if hasattr(step, 'skipped'):
                for fileobj in step.skipped.files:
                    if hasattr(fileobj, 'LogicalFileName'):
                        lfn = fileobj.LogicalFileName
                        inputType = getattr(fileobj, 'input_type', 'primaryFiles')  # Probably not working
                        files.update({lfn: {'status': 'Skipped', 'type': inputType}})

            if hasattr(step, 'fallback'):
                for fileobj in step.fallback.files:
                    if hasattr(fileobj, 'LogicalFileName'):
                        lfn = fileobj.LogicalFileName
                        inputType = getattr(fileobj, 'input_type', 'primaryFiles')  # Probably not working
                        files.update({lfn: {'status': 'Remote', 'type': inputType}})
        except AttributeError:
            return package

        inputFilesStrings = []
        fileCount = 0
        for inputFile, details in files.items():
            fileCount += 1
            success = '0'
            accessType = details['status']
            if details['status'] in ['Local', 'Remote']:
                success = '1'
            if accessType == 'Skipped':
                accessType = 'Local'
            inputFilesStrings.append('::'.join([inputFile, success, 'EDM', accessType, str(fileCount)]))

        if fileCount:
            package = {
                'inputBlocks': 'Dummy',
                'Basename': '/',
                'inputFiles': ';'.join(inputFilesStrings),
            }

        return package

    def addTask(self, task):
        """
        _addTask_

        Add a task to the Dashboard, jobs must contain the following information
        about the task:
            application -> CMSSW release
            tool -> JobSubmission tool (like Condor? or WMAgent)
            JSToolVersion -> 'tool' version
            TaskType -> Type of activity
            datasetFull -> Input dataset
            CMSUser -> owner of the workflow
        """
        taskName = task['name']
        package = {}
        package['MessageType']   = 'TaskMeta'
        package['application']   = task['application']
        package['tool']          = 'WMAgent'
        package['JSToolVersion'] = __version__
        package['TaskType']      = task['TaskType']
        package['TaskName']      = self.taskPrefix + taskName
        package['JobName']       = 'taskMeta'
        package['datasetFull']   = task['datasetFull']
        package['CMSUser']       = task['user']

        logging.info("Sending %s info" % taskName)
        logging.debug("Sending task info: %s" % str(package))

        with DashboardAPI(logr=logging, server=self.dashboardUrl) as dashboard:
            dashboard.apMonSend(package)
