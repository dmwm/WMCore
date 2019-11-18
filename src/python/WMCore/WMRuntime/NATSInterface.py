#!/usr/bin/env python
"""
NATSPublisher provides wrapper around NATS to send WMAgent messages
"""
from __future__ import division

# system modules
import socket
import logging

# WMCore modules
from WMCore.WMSpec.WMStep import WMStepHelper
from WMCore.WMSpec.WMWorkload import getWorkloadFromTask, WMWorkloadHelper
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, SiteConfigError
from WMCore.WMRuntime.Bootstrap import getSyncCE

# CMSMonitoring modules
from CMSMonitoring.NATS import NATSManager


class NATSPublisher(object):
    """
    NATSPublisher class provides generic wrapper to send NATS messages from WMAgent
    """
    def __init__(self, task, job, server, topics=None, attrs=None):
        """
        Constructor takes task and job and extract relevant information
        """
        # Basic task/job objects
        workload = getWorkloadFromTask(task)
        whelper = WMWorkloadHelper(workload)
        campaign = whelper.getCampaign()
        taskName = workload.name()
        jobName = job['name']

        siteCfg = {}
        try:
            siteCfg = loadSiteLocalConfig()
        except SiteConfigError:
            pass
        siteName = getattr(siteCfg, 'siteName', 'Unknown')
        hostName = socket.gethostname()
        ceName = getSyncCE()

        # doc we'll start with
        self.data = {'task': taskName, 'job': jobName, 'campaign': campaign,
                     'site': siteName, 'host': hostName, 'ce': ceName}

        # NATS manager
        self.nats = NATSManager(server, topics=topics, attrs=attrs, default_topic='cms.wma')
        msg = "NATS: {}".format(self.nats)
        logging.info(msg)

    def jobStart(self):
        """
        _jobStart_

        Fill with basic information upon job start, we shouldn't send anything
        until the first step starts.
        """
        # Announce that the job is running
        data = dict(self.data)
        self.nats.publish(data)

    def jobEnd(self):
        """
        _jobEnd_

        Fill with jobEnding info
        """
        data = dict(self.data)
        self.nats.publish(data)

    def jobKilled(self):
        """
        _jobKilled_

        If the job is killed let's inform its ungraceful end
        """
        data = dict(self.data)
        data['exitCode'] = 71300  # WMAgent job kill exit code
        self.nats.publish(data=data)

    def stepStart(self, step):
        """
        _stepStart_

        Fill with the step-based information. If it is the first step, report
        that the job started its execution.
        """

        helper = WMStepHelper(step)
        data = dict(self.data)
        data['step'] = helper.name()
        self.nats.publish(data)

    def stepEnd(self, step, stepReport):
        """
        _stepEnd_

        Fill with step-ending information
        """
        helper = WMStepHelper(step)
        data = dict(self.data)
        data['step'] = helper.name()
        data['exitCode'] = stepReport.getStepExitCode(stepName=helper.name())
        self.nats.publish(data=data)

    def stepKilled(self, step):
        """
        _stepKilled_

        Fill with step-ending information assuming utter failure
        """

        helper = WMStepHelper(step)
        data = dict(self.data)
        data['step'] = helper.name()
        # step should have: step.execution.exitStatus
        # we'll try to extract it , otherwise assign WMAgent job kill exit code
        exitCode = getattr(getattr(step, 'execution', ''), 'exitStatus', 71300)
        data['exitCode'] = exitCode
        self.nats.publish(data=data)

    def periodicUpdate(self):
        """
        _periodicUpdate_
        One day this will do something useful.
        """
        pass
