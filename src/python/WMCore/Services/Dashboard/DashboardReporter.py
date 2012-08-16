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
from WMCore.Services.Dashboard.DashboardAPI import apmonSend, apmonFree

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

        #Have to default this to the local host otherwise a lot of unit tests
        #die
        if hasattr(config, 'DashboardReporter'):
            self.destHost = getattr(self.config.DashboardReporter, 'dashboardHost',
                               '127.0.0.1')
            self.destPort = getattr(self.config.DashboardReporter, 'dashboardPort',
                               8884)
        else:
            self.destHost = '127.0.0.1'
            self.destPort = 8884

        self.serverreport = [self.destHost + ':' + str(self.destPort)]

        self.taskPrefix = 'wmagent_'
        self.tsFormat = '%Y-%m-%d %H:%M:%S'

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
            taskType -> Workflow type (analysis, production, etc...)
            jobType -> Job type (merge, processing, etc...)
            *NEventsToprocess -> Number of events the job will process
        Additionally the job should carry information about the task according
        to the description of the addTask method
        """
        logging.info ("Handling %d created jobs" % len(jobs))
        logging.debug ("Handling created jobs: %s" % jobs)

        for job in jobs:
            logging.debug("Sending info for job %s" % str(job))

            package = {}
            package['MessageType']      = 'JobMeta'
            package['taskId']           = self.taskPrefix + \
                                               job['workflow']
            package['jobId']            = '%s_%i' % (job['name'],
                                                    job['retry_count'])
            package['TaskType']         = job['taskType']
            package['JobType']          = job['jobType']
            package['NEventsToProcess'] = job.get('nEventsToProc',
                                                    'NotAvailable')

            logging.debug("Sending: %s" % str(package))
            result = apmonSend(taskid = package['taskId'],
                               jobid = package['jobId'],
                               params = package,
                               logr = logging,
                               apmonServer = self.serverreport)
            if result != 0:
                msg = "Error %i sending info for submitted job %s via UDP\n" \
                      % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s" \
                              % (self.destHost,
                                 self.destPort))
            apmonFree()

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
        logging.debug("Handling jobs: %s" % jobs)

        for job in jobs:
            logging.debug("Sending info for job %s" % str(job))

            package = {}
            package['MessageType']       = 'JobStatus'
            package['jobId']             = '%s_%i' % (job['name'],
                                                    job['retry_count'])
            package['taskId']            = self.taskPrefix + job['workflow']
            package['StatusValue']       = statusValue
            package['StatusValueReason'] = statusMessage
            package['StatusEnterTime']   = time.strftime(self.tsFormat,
                                        time.gmtime())
            package['StatusDestination'] = job.get('location',
                                                   'NotAvailable')

            if job.get('plugin', None):
                package['scheduler']     = job['plugin'][:-6]

            logging.debug("Sending: %s" % str(package))
            result = apmonSend(taskid = package['taskId'],
                               jobid = package['jobId'],
                               params = package,
                               logr = logging,
                               apmonServer = self.serverreport)

            if result != 0:
                msg =  "Error %i sending info for submitted job %s via UDP\n" \
                        % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s" \
                              % (self.destHost,
                                 self.destPort))
            apmonFree()

            if 'fwjr' in job:
                self.handleSteps(job)

        return

    def handleSteps(self, job):
        """
        _handleSteps_

        Handle the post-processing step information
        """
        if job['fwjr'] == None:
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

            trimmedPackage = {}
            for key in package:
                if package[key] != None:
                    trimmedPackage['%d_%s' % (counter, key)] = package[key]
            package = trimmedPackage

            if not package:
                continue

            package['jobId']    = '%s_%i' % (job['name'], job['retry_count'])
            package['taskId']   = self.taskPrefix + job['workflow']
            package['%d_stepName' % counter] = stepName


            logging.debug("Sending step info: %s" % str(package))
            result = apmonSend(taskid = package['taskId'],
                               jobid = package['jobId'], params = package,
                               logr = logging, apmonServer = self.serverreport)

            if result != 0:
                msg =  "Error %i sending info for completed job %s via UDP\n" \
                        % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s" \
                              % (self.destHost,
                                 self.destPort))
        apmonFree()

        return

    def getEventInformation(self, stepName, fwjr):
        """
        _getEventInformation_

        Handles the information about input and output files in the step
        and provides detailed event information to be sent to the dashboard
        """

        package = {}

        package['inputEvents'] = 0
        inputFiles = fwjr.getInputFilesFromStep(stepName = stepName)
        for inputFile in inputFiles:
            package['inputEvents'] += inputFile['events']

        package['OutputEventInfo'] = ''
        step = fwjr.retrieveStep(stepName)
        outputModules = getattr(step, 'outputModules', None)

        if outputModules:
            for outputMod in outputModules:
                outFiles = fwjr.getFilesFromOutputModule(step = stepName,
                                                         outputModule = outputMod)
                if not outFiles:
                    continue
                dataTier = None
                events = 0
                procDataset = None
                for outFile in outFiles:
                    if not dataTier:
                        dataTier = outFile['dataset'].get('dataTier', None)
                    if not procDataset:
                        procDataset = outFile['dataset'].get('processedDataset', None)
                    if not (dataTier and procDataset):
                        logging.error('Output module %s has a file %s with incomplete info'
                                        % (outputMod, outFile['lfn']))
                        continue
                    events += outFile['events']
                if dataTier and procDataset:
                    package['OutputEventInfo'] += '%s:%s:%d;' % (procDataset,
                                                                dataTier,
                                                                events)

        if not (package['inputEvents'] or package['OutputEventInfo']):
            return {}

        package['OutputEventInfo'] = package['OutputEventInfo'][:-1]

        return package

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
        result = apmonSend(taskid = package['TaskName'],
                           jobid = package['JobName'], params = package,
                           logr = logging, apmonServer = self.serverreport)

        if result != 0:
            msg =  "Error %i sending info for new task %s via UDP\n" % (result,
                                                                        taskName)
            msg += "Ignoring"
            logging.error(msg)
            logging.debug("Package sent: %s\n" % package)
            logging.debug("Host info: host %s, port %s" \
                          % (self.destHost,
                             self.destPort))
        apmonFree()
