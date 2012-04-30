#!/usr/bin/env python
# This is the interface to the Dashboard that the monitor will use

import threading
import os
import time
import logging
import socket

from WMCore.WMSpec.WMStep     import WMStepHelper
from WMCore.WMSpec.WMWorkload import getWorkloadFromTask
from WMCore.WMRuntime.Tools.Plugins.ApMonLite.ApMonDestMgr import ApMonDestMgr
from WMCore.Services.Dashboard.DashboardAPI import apmonSend, apmonFree
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, SiteConfigError

def getSyncCE(default = socket.gethostname()):
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

    if os.environ.has_key('NORDUGRID_CE'):
        #  //
        # // ARC, Sync CE from env. var. submitted with the job by JobSubmitter
        #//
        return os.environ['NORDUGRID_CE']

    return result

class DashboardInfo():
    """
    An object to let you assemble the information needed for a Dashboard Report

    """

    def __init__(self, task, job):
        """
        Init some stuff

        """

        #Basic task/job objects
        self.task         = task
        self.workload     = getWorkloadFromTask(task)
        self.job          = job

        #Dashboard server interface info
        self.publisher    = None
        self.destinations = {}
        self.server       = None

        #Job ids
        self.taskName = 'wmagent_%s' % self.workload.name()
        self.jobName  = '%s_%i' % (job['name'], job['retry_count'])

        #Job ending report stuff
        self.jobSuccess     = 0
        self.jobStarted     = False
        self.failedStep     = None
        self.lastStep       = None
        self.WrapperWCTime  = 0
        self.WrapperCPUTime = 0

        #Utility
        self.tsFormat = '%Y-%m-%d %H:%M:%S'

        return

    def jobStart(self):
        """
        _jobStart_

        Fill with basic information upon job start, we shouldn't send anything
        until the first step starts.
        """

        #Announce that the job is running
        data = {}
        data['MessageType']       = 'JobStatus'
        data['MessageTS']         = time.strftime(self.tsFormat, time.gmtime())
        data['taskId']            = self.taskName
        data['jobId']             = self.jobName
        data['StatusValue']       = 'running'
        data['StatusEnterTime']   = time.strftime(self.tsFormat, time.gmtime())
        data['StatusValueReason'] = 'Job started execution in the WN'
        data['StatusDestination'] = getSyncCE()

        self.publish(data = data)

        return data

    def jobEnd(self):
        """
        _jobEnd_

        Fill with jobEnding info
        """

        data = {}
        data['MessageType']    = 'jobRuntime-jobEnd'
        data['MessageTS']      = time.strftime(self.tsFormat, time.gmtime())
        data['taskId']         = self.taskName
        data['jobId']          = self.jobName
        data['ExeEnd']         = self.lastStep
        data['WrapperCPUTime'] = self.WrapperCPUTime
        data['WrapperWCTime']  = self.WrapperWCTime
        data['JobExitCode']    = self.jobSuccess
        if self.failedStep:
            data['JobExitReason'] = 'Step %s failed in the WN' % self.failedStep
        else:
            data['JobExitReason'] = 'Job completed execution in the WN'

        self.publish(data = data)

        return data

    def stepStart(self, step):
        """
        _stepStart_

        Fill with the step-based information. If it is the first step, report
        that the job started its execution.
        """

        helper = WMStepHelper(step)
        data = None
        if not self.jobStarted:
            #It's the first step so let's send the exe that started and where
            #That's what they request
            data = {}
            data['MessageType']       = 'jobRuntime-jobStart'
            data['MessageTS']         = time.strftime(self.tsFormat,
                                                      time.gmtime())
            data['taskId']            = self.taskName
            data['jobId']             = self.jobName
            data['ExeStart']          = helper.name()
            data['SyncCE']            = getSyncCE()
            data['WNHostName']        = socket.gethostname()

            self.publish(data = data)
            self.jobStarted = True

        #Now let's send the step information
        tmp = {'jobStart': data}
        
        data = {}
        data['MessageType'] = 'jobRuntime-stepStart'
        data['MessageTS']   = time.strftime(self.tsFormat, time.gmtime())
        data['taskId']      = self.taskName
        data['jobId']       = self.jobName
        data['ExeStart']    = helper.name()

        self.publish(data = data)
        
        data.update(tmp)
        
        return data

    def stepEnd(self, step, stepReport):
        """
        _stepEnd_

        Fill with step-ending information
        """
        helper = WMStepHelper(step)

        stepSuccess = stepReport.getStepExitCode(stepName = helper.name())
        if self.jobSuccess == 0:
            self.jobSuccess = int(stepSuccess)
            self.failedStep = helper.name()

        data = {}
        data['MessageType']              = 'jobRuntime-stepEnd'
        data['MessageTS']                = time.strftime(self.tsFormat,
                                                         time.gmtime())
        data['taskId']                   = self.taskName
        data['jobId']                    = self.jobName
        data['ExeEnd']                   = helper.name()
        data['ExeExitCode']              = stepReport.getStepExitCode(
                                                       stepName = helper.name())
        if helper.name() == 'StageOut':
            data['StageOutExitStatus']   = int(stepReport.stepSuccessful(
                                                      stepName = helper.name()))

        times = stepReport.getTimes(stepName = helper.name())
        data['ExeWCTime'] = times['stopTime'] - times['startTime']

        step = stepReport.retrieveStep(step = helper.name())

        if hasattr(step, 'performance'):
            if hasattr(step.performance, 'cpu'):
                data['ExeCPUTime'] = getattr(step.performance.cpu,
                                             'TotalJobCPU', 0)
                self.WrapperCPUTime += data['ExeCPUTime']

        self.WrapperWCTime += data['ExeWCTime']
        self.lastStep = helper.name()

        self.publish(data = data)

        return data

    def jobKilled(self):
        """
        _jobKilled_

        If the job is killed let's inform its ungraceful end
        """

        data = {}
        data['MessageType']    = 'jobRuntime-jobEnd'
        data['MessageTS']      = time.strftime(self.tsFormat, time.gmtime())
        data['taskId']         = self.taskName
        data['jobId']          = self.jobName
        data['ExeEnd']         = self.lastStep
        data['WrapperCPUTime'] = self.WrapperCPUTime
        data['WrapperWCTime']  = self.WrapperWCTime
        data['JobExitCode']    = 9999
        data['JobExitReason']  = 'Job was killed in the WN'

        self.publish(data = data)

        return data

    def stepKilled(self, step):
        """
        _stepKilled_

        Fill with step-ending information assuming utter failure
        """

        helper = WMStepHelper(step)

        data = {}
        data['MessageType']   = 'jobRuntime-stepKilled'
        data['MessageTS']     = time.strftime(self.tsFormat, time.gmtime())
        data['taskId']        = self.taskName
        data['jobId']         = self.jobName
        data['ExeEnd']        = helper.name()

        self.lastStep = helper.name()

        self.publish(data = data)

        return data

    def periodicUpdate(self):
        """
        _periodicUpdate_

        One day this will do something useful.
        But not yet
        """

        return

    def addDestination(self, host, port):
        """
        _addDestination_

        Add a publishing destination to the Publisher
        """

        if self.publisher == None:
            self._InitPublisher()
        self.destinations[host] = port
        self.publisher.newDestination(host, port)
        self.server = ['%s:%s' % (host, port)]

    def publish(self, data, redundancy = 1):
        """
        _publish_

        Publish information in this object to the Dashboard
        using the ApMon interface and the destinations stored in this
        instance.

        redunancy is the amount to times to publish this information

        """

        logging.debug("About to send UDP package to dashboard: %s" % data)
        logging.debug("Using address %s" % self.server)
        apmonSend(taskid = self.taskName, jobid = self.jobName, params = data,
                  logr = logging, apmonServer = self.server)
        apmonFree()
        return

    def _InitPublisher(self):
        """
        _InitPublisher_

        *private*

        Initialise the ApMonDestMgr instance, verifying that the task and
        job attributes are set

        """
        if self.taskName == None:
            msg = "Error: You must set the task id before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError, msg
        if self.jobName == None:
            msg = "Error: You must set the job id before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError, msg
        self.publisher = ApMonDestMgr(clusterName = self.taskName,
                                      nodeName = self.jobName)
        return
