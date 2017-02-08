from __future__ import (division, print_function)
from WMCore.REST.HeartbeatMonitorBase import HeartbeatMonitorBase
from WMCore.WorkQueue.WorkQueue import globalQueue

class HeartbeatMonitor(HeartbeatMonitorBase):

    def addAdditionalMonitorReport(self, config):
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
        
        return results