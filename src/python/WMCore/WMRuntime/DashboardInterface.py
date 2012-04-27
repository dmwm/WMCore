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


def getSyncCE(default = socket.gethostname()):
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

    # Stu says LCG may have the globus gram contact

    #if os.environ.has_key('EDG_WL_JOBID'):
    #    #  //
    #    # // LCG, Sync CE from edg command
    #    #//
    #    command = "glite-brokerinfo getCE"
    #    pop = popen2.Popen3(command)
    #    pop.wait()
    #    exitCode = pop.poll()
    #    if exitCode:
    #        return result
    #
    #    content = pop.fromchild.read()
    #    result = content.strip()
    #    return result

    if os.environ.has_key('NORDUGRID_CE'):
        #  //
        # // ARC, Sync CE from env. var. submitted with the job by JobSubmitter
        #//
        return os.environ['NORDUGRID_CE']

    return result




class DashboardInfo(dict):
    """
    An object to let you assemble the information needed for a Dashboard Report

    """


    def __init__(self, task, job):
        """
        Init some stuff

        """


        self.task         = task
        self.workload     = getWorkloadFromTask(task)
        self.job          = job
        self.publisher    = None
        self.destinations = {}
        self.server       = None
        self.agentName    = getattr(self.workload.data, 'WMAgentName',
                                    'WMAgentPrimary')

        dict.__init__(self)


        self.setdefault("Application", None)
        self.setdefault("ApplicationVersion", None)
        self.setdefault("GridJobID", None)
        self.setdefault("LocalBatchID", None)
        self.setdefault("GridUser", None)
        self.setdefault("User" , self.workload.getOwner().get('name', 'WMAgent'))
        self.setdefault("JSTool","WMAgent")
        self.setdefault("NbEvPerRun", 0)
        self.setdefault("NodeName", None)
        self.setdefault("Scheduler", None)
        self.setdefault("TaskType", self.task.taskType())
        self.setdefault("NSteps", 0)
        self.setdefault("VO", "CMS")
        self.setdefault("TargetCE", None)
        self.setdefault("RBname", None)
        self.setdefault("JSToolUI" , None) # Can't set here, see bug #64232


        self.taskName = 'wmagent_%s' % self.workload.name()
        self.jobName  = '%s_%i' % (job['name'], job['retry_count'])
        self.jobSuccess = 0
        self.jobStarted = False
        self.failedStep = None
        self.tsFormat = '%Y-%m-%d %H:%M:%S'
        self.lastStep = None

        #Need to consider all possible steps
        self.WrapperWCTime = 0
        self.WrapperCPUTime = 0


        return


    def jobStart(self):
        """
        _jobStart_

        Fill with basic information upon job start, we shouldn't send anything
        until the first step starts.
        """

        #Announce that the job is running'
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

        return


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
        if self.jobStarted:
            #It's the first step so let's send the exe that started and where
            #That's what they request
            data = {}
            data['MessageType']       = 'jobRuntime-jobStart'
            data['MessageTS']         = time.strftime(self.tsFormat,
                                                      time.timegm())
            data['taskId']            = self.taskName
            data['jobId']             = self.jobName
            data['ExeStart']          = helper.name()
            data['SyncCE']            = getSyncCE()
            data['WNHostName']        = socket.gethostname()

            self.publish(data = data)
            self.jobStarted = True

        #Now let's send the step information
        data = {}
        data['MessageType'] = 'jobRuntime-stepStart'
        data['MessageTS']   = time.strftime(self.tsFormat, time.timegm())
        data['taskId']      = self.taskName
        data['jobId']       = self.jobName
        data['ExeStart']    = helper.name()

        self.publish(data = data)

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
                                                         time.timegm())
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
        data['JobExitReason']  = 'Job was killed in the WN'

        self.publish(data = data)

        return

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


        return


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



    def publish(self, redundancy = 1, data = None):
        """
        _publish_

        Publish information in this object to the Dashboard
        using the ApMon interface and the destinations stored in this
        instance.

        redunancy is the amount to times to publish this information

        """
        toPublish = {}
        if data:
            toPublish = data
        else:
            toPublish.update(self)
        for key, value in toPublish.items():
            if value == None:
                del toPublish[key]


        logging.debug("About to send UDP package to dashboard: %s" % toPublish)
        logging.debug("Using address %s" % self.server)
        apmonSend(taskid = self.taskName, jobid = self.jobName, params = toPublish,
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
        self.publisher = ApMonDestMgr(clusterName = self.taskName, nodeName = self.jobName)
        return







