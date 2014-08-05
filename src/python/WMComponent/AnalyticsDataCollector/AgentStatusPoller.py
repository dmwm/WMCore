"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
import traceback
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Database.CMSCouch import CouchMonitor
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import LocalCouchDBData, \
     WMAgentDBData, combineAnalyticsData, convertToRequestCouchDoc, \
     convertToAgentCouchDoc, isDrainMode, initAgentInfo, DataUploadTime, \
     diskUse, numberCouchProcess
from WMCore.WMFactory import WMFactory

class AgentStatusPoller(BaseWorkerThread):
    """
    Gether the summary data for request (workflow) from local queue,
    local job couchdb, wmbs/boss air and populate summary db for monitoring
    """
    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        # set the workqueue service for REST call
        self.config = config
        # need to get campaign, user, owner info
        self.agentInfo = initAgentInfo(self.config)
        self.summaryLevel = (config.AnalyticsDataCollector.summaryLevel).lower()
            
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        # set the connection for local couchDB call
        #self.localSummaryCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL)
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)
        
        self.localCouchServer = CouchMonitor(self.config.JobStateMachine.couchurl)

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Getting Agent info ...")
            agentInfo = self.collectAgentInfo()
            
            #set the uploadTime - should be the same for all docs
            uploadTime = int(time.time())
            
            self.uploadAgentInfoToCentralWMStats(agentInfo, uploadTime)
            
            logging.info("Agent components down:\n %s" % agentInfo['down_components'])
            logging.info("Agent in drain mode:\n %s \nsleep for next WMStats alarm updating cycle"
                          % agentInfo['drain_mode'])
            
        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
    
    def collectAgentInfo(self):
        #TODO: agent info (need to include job Slots for the sites)
        # always checks couch first
        source = self.config.JobStateMachine.jobSummaryDBName
        target = self.config.AnalyticsDataCollector.centralWMStatsURL
        couchInfo = self.localCouchServer.recoverReplicationErrors(source, target, 
                                                                   filter = "WMStats/repfilter")
        logging.info("getting couchdb replication status: %s" % couchInfo)
        
        agentInfo = self.wmagentDB.getComponentStatus(self.config)
        agentInfo.update(self.agentInfo)
        
        if isDrainMode(self.config):
            logging.info("Agent is in DrainMode")
            agentInfo['drain_mode'] = True
            agentInfo['status'] = "warning"
        
        else:
            agentInfo['drain_mode'] = False
        
        if (couchInfo['status'] != 'ok'):
            agentInfo['down_components'].append("CouchServer")
            agentInfo['status'] = couchInfo['status']
            couchInfo['name'] = "CouchServer"
            agentInfo['down_component_detail'].append(couchInfo)
        
        
        # Disk space warning   
        diskUseList = diskUse()
        diskUseThreshold = float(self.config.AnalyticsDataCollector.diskUseThreshold)
        agentInfo['disk_warning'] = []
        for disk in diskUseList:
            if float(disk['percent'].strip('%')) >= diskUseThreshold and disk['mounted'] not in self.config.AnalyticsDataCollector.ignoreDisk:
                agentInfo['disk_warning'].append(disk)
        
        # Couch process warning
        couchProc = numberCouchProcess()
        couchProcessThreshold = float(self.config.AnalyticsDataCollector.couchProcessThreshold)
        if couchProc >= couchProcessThreshold:
            agentInfo['couch_process_warning'] = couchProc
        else:
            agentInfo['couch_process_warning'] = 0
        
        # This adds the last time and message when data was updated to agentInfo
        lastDataUpload = DataUploadTime.getInfo(self)
        if lastDataUpload['data_last_update']!=0:
            agentInfo['data_last_update'] = lastDataUpload['data_last_update']
        if lastDataUpload['data_error']!="":
            agentInfo['data_error'] = lastDataUpload['data_error']
        
        # Change status if there is data_error, couch process maxed out or disk full problems.
        if agentInfo['status'] == 'ok':
            if agentInfo['disk_warning'] != []:
                agentInfo['status'] = "warning"
                
        if agentInfo['status'] == 'ok' or agentInfo['status'] == 'warning':
            if (agentInfo.has_key('data_error') and agentInfo['data_error'] != 'ok') or \
               (agentInfo.has_key('couch_process_warning') and agentInfo['couch_process_warning'] != 0):
                agentInfo['status'] = "error"

        return agentInfo

    def uploadAgentInfoToCentralWMStats(self, agentInfo, uploadTime):
        #direct data upload to the remote to prevent data conflict when agent is cleaned up and redeployed
        agentDocs = convertToAgentCouchDoc(agentInfo, self.config.ACDC, uploadTime)
        self.centralWMStatsCouchDB.updateAgentInfo(agentDocs)

