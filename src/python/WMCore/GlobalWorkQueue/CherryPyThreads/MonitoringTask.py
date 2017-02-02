from __future__ import (division, print_function)

from Utils.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter, convertToServiceCouchDoc


class MonitoringTask(CherryPyPeriodicTask):
    def __init__(self, rest, config):
        super(MonitoringTask, self).__init__(config, enableLogDB=True)
        self.centralWMStats = WMStatsWriter(config.queueParams['WMStatsCouchUrl'])

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.monitorGlobalQueue, 'duration': config.monitDuration}]

    def monitorGlobalQueue(self, config):
        """
        Collect some statistics for Global Workqueue and upload it to WMStats. They are:
        - by status: count of elements and total number of estimated jobs
        - by status: count of elements and sum of jobs by *priority*.
        - by agent: count of elements and sum of jobs by *status*
        - by agent: count of elements and sum of jobs by *priority*
        - by status: unique (distributed) and possible (total assigned) number
          of jobs and elements per *site*, taking into account data locality
        - by status: unique (distributed) and possible (total assigned) number
          of jobs and elements per *site*, regardless data locality (using AAA)

        TODO: these still need to be done
        * for Available workqueue elements:
         - WQE without a common site list (that does not pass the work restrictions)
         - WQE older than 7 days (or whatever number we decide)
         - WQE that create > 30k jobs (or whatever number we decide)
        * for Acquired workqueue elements
         - WQE older than 7 days (or whatever the number is)
        """
        self.logger.info("Collecting GlobalWorkqueue statistics...")

        # retrieve whole docs for these status in order to create site metrics
        status = ['Available', 'Negotiating', 'Acquired']
        globalQ = globalQueue(**config.queueParams)
        results = globalQ.monitorWorkQueue(status)

        wqSummaryDoc = convertToServiceCouchDoc(results, config.queueParams['log_reporter'])
        self.centralWMStats.updateAgentInfo(wqSummaryDoc)

        return
