#!/usr/bin/env python
# This is the interface to the Dashboard that the monitor will use

from future import standard_library
standard_library.install_aliases()

import threading
import os
import time
import logging
import socket
import subprocess
import re

from WMCore.WMSpec.WMStep import WMStepHelper
from WMCore.WMSpec.WMWorkload import getWorkloadFromTask
from WMCore.Services.Dashboard.DashboardAPI import DashboardAPI
from WMCore.Services.Dashboard.DashboardReporter import unicodeToStr
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, SiteConfigError


def getSyncCE(default=socket.gethostname()):
    """
    _getSyncCE_

    Tries to get the site name from the localSite config, if it doesn't find it
    or it finds an empty string then we check the environment
    variables. Worst case scenario we give the Worker node.

    """

    try:
        siteConfig = loadSiteLocalConfig()
        result = siteConfig.siteName
        if result:
            return result
    except SiteConfigError:
        logging.error("Couldn't find the site config, looking for the CE elsewhere")

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
            logging.warning("Failed to parse GLOBUS_GRAM_JOB_CONTACT env var")
        return result

    if 'NORDUGRID_CE' in os.environ:
        #  //
        # // ARC, Sync CE from env. var. submitted with the job by JobSubmitter
        # //
        return os.environ['NORDUGRID_CE']

    return result


def _commandWrapper(command, process):
    """
    __commandWrapper_

    Wrapper to execute a command using subprocess, the process object,
    stdout and stderr can be retrieved from the process dictionary
    """
    try:
        process['process'] = subprocess.Popen(command,
                                              stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
        process['stdout'], process['stderr'] = process['process'].communicate()
    except:
        pass


def _executeCommand(command, timeout):
    """
    __executeCommand_

    Executes the command on a separate thread to prevent deadlocks,
    if the command takes more than the timeout to end then it will be
    terminated
    """

    process = {'process': None, 'stdout': None, 'stderr': None}
    thread = threading.Thread(target=_commandWrapper,
                              args=(command, process))
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        if process['process']:
            process['process'].terminate()
            thread.join()
            logging.error('Command: %s timed out, return code: %i'
                          % (' '.join(command), process['process'].returncode))
        else:
            # Process was never initiated, there was an error
            # bail out
            logging.error('Command : %s could not be executed' % ' '.join(command))
        return None
    else:
        return process['stdout']


def _parseDN(subject):
    """
    __parseDN_

    Find a valid certificate subject in the given string,
    and if found strip the extra proxy strings from it
    """
    proxy = r'/CN=proxy'
    limitedProxy = r'/CN=limited proxy'
    dn = r'(?:(?:/[A-Za-z0-9=_\\/\s]+)+)'
    subject = re.sub(proxy, '', subject)
    subject = re.sub(limitedProxy, '', subject)
    match = re.findall(dn, subject)
    if match:
        try:
            return match[0]
        except:
            pass
    return None


def getUserProxyDN():
    """
    _getUserProxyDN_

    Looks for the subject of the user proxy, and returns it.
    The locations the method searches for the DN are:
     1. grid-proxy-info --subject
     2. openssl x509 -subject -noout -in $X509_USER_PROXY
    If it can not be found returns None
    """
    timeout = 300

    subject = None
    command = ['grid-proxy-info', '-subject']
    subject = _executeCommand(command, timeout)

    if not subject and 'X509_USER_PROXY' in os.environ:
        command = ['openssl', 'x509', '-subject',
                   '-noout', '-in', os.environ['X509_USER_PROXY']]
        subject = _executeCommand(command, timeout)

    if subject:
        subject = _parseDN(subject)

    return subject


class DashboardInfo(object):
    """
    An object to let you assemble the information needed for a Dashboard Report

    """

    def __init__(self, task, job, dashboardUrl=None, overrideCores=0):
        """
        Init some stuff

        """

        # Basic task/job objects
        self.task = task
        self.workload = getWorkloadFromTask(task)
        self.job = job

        # Dashboard server interface info
        self.publisher = None
        self.destinations = {}
        self.server = dashboardUrl

        # Job ids
        self.taskName = unicodeToStr('wmagent_%s' % self.workload.name())
        self.jobName = unicodeToStr('%s_%i' % (job['name'], job['retry_count']))

        # Step counter
        self.stepCount = 0

        # Job ending report stuff
        self.jobSuccess = 0
        self.jobStarted = False
        self.failedStep = None
        self.lastStep = None
        self.maxCores = overrideCores  # Accumulated over individual steps
        self.WrapperWCTime = 0
        self.WrapperCPUTime = 0

        # Utility
        self.tsFormat = '%Y-%m-%d %H:%M:%S'

        return

    def jobStart(self):
        """
        _jobStart_

        Fill with basic information upon job start, we shouldn't send anything
        until the first step starts.
        """
        # Announce that the job is running
        data = {}
        data['MessageType'] = 'JobStatus'
        data['MessageTS'] = time.strftime(self.tsFormat, time.gmtime())
        data['taskId'] = self.taskName
        data['jobId'] = self.jobName
        data['StatusValue'] = 'running'
        data['StatusEnterTime'] = time.strftime(self.tsFormat, time.gmtime())
        data['StatusValueReason'] = 'Job started execution in the WN'
        data['StatusDestination'] = getSyncCE()

        # If number of cores is not overriden from the watchdog, then find the
        # highest number of cores among all the steps and send it to dashboard
        if self.maxCores == 0:
            for stepName in self.task.listAllStepNames():
                sh = self.task.getStepHelper(stepName)
                self.maxCores = max(self.maxCores, sh.getNumberOfCores())
        data['NCores'] = self.maxCores

        self.publish(data=data)

        return data

    def jobEnd(self):
        """
        _jobEnd_

        Fill with jobEnding info
        """

        data = {}
        data['MessageType'] = 'jobRuntime-jobEnd'
        data['MessageTS'] = time.strftime(self.tsFormat, time.gmtime())
        data['taskId'] = self.taskName
        data['jobId'] = self.jobName
        data['ExeEnd'] = self.lastStep
        data['NCores'] = self.maxCores
        data['WrapperCPUTime'] = self.WrapperCPUTime
        data['WrapperWCTime'] = self.WrapperWCTime
        data['JobExitCode'] = self.jobSuccess
        if self.failedStep:
            data['JobExitReason'] = 'Step %s failed in the WN' % self.failedStep
        else:
            data['JobExitReason'] = 'Job completed execution in the WN'

        self.publish(data=data)

        return data

    def stepStart(self, step):
        """
        _stepStart_

        Fill with the step-based information. If it is the first step, report
        that the job started its execution.
        """

        helper = WMStepHelper(step)
        self.stepCount += 1

        data = None
        if not self.jobStarted:
            # It's the first step so let's send the exe that started and where
            # That's what they request
            data = {}
            data['MessageType'] = 'jobRuntime-jobStart'
            data['MessageTS'] = time.strftime(self.tsFormat,
                                              time.gmtime())
            data['taskId'] = self.taskName
            data['jobId'] = self.jobName
            data['GridName'] = getUserProxyDN()
            data['ExeStart'] = helper.name()
            data['SyncCE'] = getSyncCE()
            data['WNHostName'] = socket.gethostname()

            self.publish(data=data)
            self.jobStarted = True

        # Save the jobStart data for unit tests
        tmp = {'jobStart': data}

        # Now let's send the step information
        data = {}
        data['MessageType'] = 'jobRuntime-stepStart'
        data['MessageTS'] = time.strftime(self.tsFormat, time.gmtime())
        data['taskId'] = self.taskName
        data['jobId'] = self.jobName
        data['%d_stepName' % self.stepCount] = helper.name()
        data['%d_ExeStart' % self.stepCount] = helper.name()

        self.publish(data=data)

        data.update(tmp)
        return data

    def stepEnd(self, step, stepReport):
        """
        _stepEnd_

        Fill with step-ending information
        """
        helper = WMStepHelper(step)

        stepSuccess = stepReport.getStepExitCode(stepName=helper.name())
        stepReport.setStepCounter(stepName=helper.name(), counter=self.stepCount)
        if self.jobSuccess == 0:
            self.jobSuccess = int(stepSuccess)
        if int(stepSuccess) != 0:
            self.failedStep = helper.name()

        data = {}
        data['MessageType'] = 'jobRuntime-stepEnd'
        data['MessageTS'] = time.strftime(self.tsFormat,
                                          time.gmtime())
        data['taskId'] = self.taskName
        data['jobId'] = self.jobName
        data['%d_ExeEnd' % self.stepCount] = helper.name()
        data['%d_ExeExitCode' % self.stepCount] = stepReport.getStepExitCode(
            stepName=helper.name())
        data['%d_NCores' % self.stepCount] = helper.getNumberOfCores()

        if helper.name() == 'StageOut':
            data['%d_StageOutExitStatus' % self.stepCount] = int(
                stepReport.stepSuccessful(stepName=helper.name()))

        times = stepReport.getTimes(stepName=helper.name())
        if times['stopTime'] is not None and times['startTime'] is not None:
            data['%d_ExeWCTime' % self.stepCount] = times['stopTime'] - times['startTime']
        else:
            logging.error('Failed to retrieve start/end step time: %s', times)
            data['%d_ExeWCTime' % self.stepCount] = 0
        self.WrapperWCTime += data['%d_ExeWCTime' % self.stepCount]

        step = stepReport.retrieveStep(step=helper.name())
        try:
            data['%d_ExeCPUTime' % self.stepCount] = getattr(step.performance.cpu,
                                                             'TotalJobCPU', 0)
        except AttributeError:
            msg = "Failed to retrieve cpu performance for step %s. Defaulting to 0" % helper.name()
            logging.warn(msg)
            data['%d_ExeCPUTime' % self.stepCount] = 0
        self.WrapperCPUTime += float(data['%d_ExeCPUTime' % self.stepCount])

        self.lastStep = helper.name()

        self.publish(data=data)

        return data

    def jobKilled(self):
        """
        _jobKilled_

        If the job is killed let's inform its ungraceful end
        """

        data = {}
        data['MessageType'] = 'jobRuntime-jobEnd'
        data['MessageTS'] = time.strftime(self.tsFormat, time.gmtime())
        data['taskId'] = self.taskName
        data['jobId'] = self.jobName
        data['ExeEnd'] = self.lastStep
        data['WrapperCPUTime'] = self.WrapperCPUTime
        data['WrapperWCTime'] = self.WrapperWCTime
        data['JobExitCode'] = 9999
        data['JobExitReason'] = 'Job was killed in the WN'

        self.publish(data=data)

        return data

    def stepKilled(self, step):
        """
        _stepKilled_

        Fill with step-ending information assuming utter failure
        """

        helper = WMStepHelper(step)

        data = {}
        data['MessageType'] = 'jobRuntime-stepKilled'
        data['MessageTS'] = time.strftime(self.tsFormat, time.gmtime())
        data['taskId'] = self.taskName
        data['jobId'] = self.jobName
        data['%d_ExeEnd' % self.stepCount] = helper.name()

        self.lastStep = helper.name()

        self.publish(data=data)

        return data

    def periodicUpdate(self):
        """
        _periodicUpdate_

        One day this will do something useful.
        But not yet
        """

        return

    def publish(self, data):
        """
        _publish_

        Publish information in this object to the Dashboard
        using the ApMon interface and the destinations stored in this
        instance.
        """
        logging.info("About to send UDP package to dashboard: %s" % data)
        with DashboardAPI(server=self.server) as dashboard:
            logging.info("Using address %s" % dashboard.server)
            dashboard.apMonSend(data)

        return
