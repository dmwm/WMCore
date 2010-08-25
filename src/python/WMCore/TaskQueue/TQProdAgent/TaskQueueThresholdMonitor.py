#!/usr/bin/env python
"""
_TaskQueueThresholdMonitor_

Checks the thresholds associated with a fake site representing the
TaskQueue: TASK_QUEUE. It returns count-only contraints (i.e. with
no site, job type or workflow).
"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor
import logging

from TQComp.Apis.TQStateApi import TQStateApi
from WMCore import Configuration as WMCoreConfig
from ProdAgentCore.Configuration import loadProdAgentConfiguration


# TODO: Is there a global defaults file (like we have in TaskQueue) ?

# How many resources to publish at each iteration
DEFAULT_BLOCK_SIZE = 20
# Below this number of total tasks, ignore other thresholds and publish
DEFAULT_ALL_PASS = 100
# Maximum number of queued tasks to publish new resources (0 for infinite)
DEFAULT_MAX_QUEUED = 0
# Maximum number of total tasks to publish new resources (0 for infinite)
DEFAULT_MAX_TOTAL = 0
# Maximum number of active tasks to publish new resources (0 for infinite)
DEFAULT_MAX_ACTIVE = 0
# Minimum percentage of active tasks versus total (active + queued)
# to publish new resources 
DEFAULT_ACTIVE_RATE = 30



class TaskQueueThresholdMonitor(MonitorInterface):
    """
    Checks the thresholds associated with a fake site representing the
    TaskQueue: TASK_QUEUE ands returns count-only contraints (i.e. with
    no site, job type or workflow).
    """

    def __init__(self):
        # TODO: The ResourceMonitor recreates this object in every call
        # Is there a way to reuse the DB interface?
        """
        _TaskQueueThresholdMonitor_

        This enhanced constructor creates an API to the TaskQueue (not to 
        recreate it every time it is called). 
        """
        # Call our parent
        MonitorInterface.__init__(self)

        logging.debug("<<<<< Creating TaskQueueThresholdMonitor>>>>>")

        self.me = "TaskQueueThresholdMonitor"
        # Try to get the TaskQueue conf file from ProdAgentConfig.xml
        what = "TaskQueue"
        try:
            cfg = loadProdAgentConfiguration()
            self.tqConfig = cfg.getConfig(what)
        except StandardError, ex:
            msg = "%s.Config:" % what
            msg += "Unable to load ProdAgent Config for " + what
            msg += "%s\n" % ex
            logging.critical(msg)
            raise ProdAgentException(msg)

        # Now load the TaskQueue API
        confFile = self.tqConfig['TaskQueueConfFile']
        myconfig = WMCoreConfig.loadConfigurationFile(confFile)
        self.tqApi = TQStateApi(logging, myconfig, None)
        logging.debug("<<<<< TaskQueueThresholdMonitor created! >>>>>")
   
    
    def __call__(self):
        """
        _operator()_
        """

        result = []

        # Retrieve the TaskQueue resource and thresholds
        if "TASK_QUEUE" not in self.activeSites:
            msg = "No TASK_QUEUE resource is active in ResourceDB"
            msg += ". Cannot verify thresholds."
            logging.warning(msg)
            return []
        self.thrs = self.siteThresholds["TASK_QUEUE"]

        msg = "%s: ResourceDB thresholds: %s" % (self.me, self.thrs)
        logging.debug(msg)


        # Get the number of queued and running tasks at the TaskQueue
        taskCounts = self.tqApi.getTaskCounts()
        active = float(taskCounts['running'])
        queued = float(taskCounts['queued'])
        total  = active + queued
        logging.debug('Task counts - active, queued, total: %s, %s, %s' % (active, queued, total))

        if total:
            ratio = 100 * active/total
        else:
            ratio = 100

        # Get values for thresholds (resourceDB or defaults)
        if self.thrs.has_key('jobBlockSize'):
            self.blockSize = int(self.thrs['jobBlockSize'])
        else:
            self.blockSize = DEFAULT_BLOCK_SIZE
            
        if self.thrs.has_key('allPassThreshold'):
            allPass = int(self.thrs['allPassThreshold'])
        else:
            allPass = DEFAULT_ALL_PASS
            
        if self.thrs.has_key('maxQueuedThreshold'):
            maxQueued = int(self.thrs['maxQueuedThreshold'])
        else:
            maxQueued = DEFAULT_MAX_QUEUED
            
        if self.thrs.has_key('maxTotalThreshold'):
            maxTotal = int(self.thrs['maxTotalThreshold'])
        else:
            maxTotal = DEFAULT_MAX_TOTAL

        if self.thrs.has_key('maxActiveThreshold'):
            maxActive = int(self.thrs['maxActiveThreshold'])
        else:
            maxActive = DEFAULT_MAX_ACTIVE

        if self.thrs.has_key('minActiveRateThreshold'):
            actvRatio = int(self.thrs['minActiveRateThreshold'])
        else:
            actvRatio = DEFAULT_ACTIVE_RATE
            
        msg = "%s: Thresholds: %s, %s, %s, %s, %s, %s" % (self.me, \
           self.blockSize, allPass, maxQueued, maxActive, maxTotal, actvRatio)
        logging.debug(msg)
 
        # Apply policy based on defined thresholds (either publish or pass)
        if total <= allPass:
            msg = "%s: Total tasks (%s) below allPassThr" % (self.me, total)
            logging.debug(msg)
            result = self.prepare()
            return result

        if maxQueued and (queued > maxQueued):
            msg = "%s: Queued tasks (%s) over maxQueuedThr" % (self.me, queued)
            logging.debug(msg)
            return []

        if maxActive and (active > maxActive):
            msg = "%s: Active tasks (%s) over maxActiveThr" % (self.me, active)
            logging.debug(msg)
            return []
       
        if maxTotal and (total > maxTotal):
            msg = "%s: Total tasks (%s) over maxTotalThr" % (self.me, total)
            logging.debug(msg)
            return []
       
        if (ratio) >= actvRatio:
            msg = "%s: Active/total (%s) over threshold" % (self.me, ratio)
            logging.debug(msg)
            result = self.prepare()
            return result
        else:
            msg = "%s: Active/total (%s) below threshold" % (self.me, ratio)
            logging.debug(msg)

        return []


    def prepare(self):
        msg = "%s: New constraint of %s" % (self.me, self.blockSize)
        logging.debug(msg)
        constraint = self.newConstraint()
        constraint['count'] = self.blockSize
        return [constraint]



registerMonitor(TaskQueueThresholdMonitor, TaskQueueThresholdMonitor.__name__)
