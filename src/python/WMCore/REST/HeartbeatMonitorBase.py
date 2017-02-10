from __future__ import (division, print_function)
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter, convertToServiceCouchDoc

class HeartbeatMonitorBase(CherryPyPeriodicTask):
    
    def __init__(self, rest, config):
        super(HeartbeatMonitorBase, self).__init__(config)
        self.centralWMStats = WMStatsWriter(config.wmstats_url)
        self.threadList = config.thread_list

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.reportToWMStats, 'duration': config.heartbeatCheckDuration}]
        
    def reportToWMStats(self, config):
        """
        report thread status and heartbeat. 
        Also can report additional mointoring information by rewriting addAdditionalMonitorReport method
        """
        self.logger.info("Checking Thread status...")
        downThreadInfo = self.logDB.wmstats_down_components_report(self.threadList)
        monitorInfo = self.addAdditionalMonitorReport(config)
        downThreadInfo.update(monitorInfo)
        wqSummaryDoc = convertToServiceCouchDoc(downThreadInfo, config.log_reporter)
        self.centralWMStats.updateAgentInfo(wqSummaryDoc)
        
        self.logger.info("Uploaded to WMStats...")

        return
    
    def addAdditionalMonitorReport(self, config):
        """
        add Additonal report with heartbeat report
        overwite the method with each applications monitoring info. (Need to follow the format displayed in wmstats)
        """
        return {}