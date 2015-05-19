"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
import traceback
from WMCore.Lexicon import sanitizeURL
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Database.CMSCouch import CouchMonitor
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import WMAgentDBData, \
     convertToAgentCouchDoc, isDrainMode, initAgentInfo, DataUploadTime, \
     diskUse, numberCouchProcess

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
    
    def setUpCouchDBReplication(self):
        
        self.replicatorDocs = []
        #TODO: tier0 specific code - need to make it generic 
        if hasattr(self.config, "Tier0Feeder"):
            t0Source = self.config.Tier0Feeder.requestDBName
            t0Target = self.config.AnalyticsDataCollector.centralRequestDBURL
            self.replicatorDocs.append({'source': t0Source, 'target': t0Target, 
                                        'filter': "T0Request/repfilter"})
        else: # set up workqueue replication
            #TODO: tier0 specific code - need to make it generic 
            wmstatsSource = self.config.JobStateMachine.jobSummaryDBName
            wmstatsTarget = self.config.AnalyticsDataCollector.centralWMStatsURL
            
            self.replicatorDocs.append({'source': wmstatsSource, 'target': wmstatsTarget, 
                                        'filter':  "WMStatsAgent/repfilter"})
            
            wqfilter = 'WorkQueue/queueFilter'
            parentQURL = self.config.WorkQueueManager.queueParams["ParentQueueCouchUrl"]
            childURL = self.config.WorkQueueManager.queueParams["QueueURL"]
            query_params = {'childUrl' : childURL, 'parentUrl' : sanitizeURL(parentQURL)['url']}
            localQInboxURL = "%s_inbox" % self.config.AnalyticsDataCollector.localQueueURL
            self.replicatorDocs.append({'source': sanitizeURL(parentQURL)['url'], 'target': localQInboxURL, 
                                        'filter': wqfilter, 'query_params': query_params})       
            self.replicatorDocs.append({'source': sanitizeURL(localQInboxURL)['url'], 'target': parentQURL, 
                                        'filter': wqfilter, 'query_params': query_params})
        
        # delete or replicator docs befor setting up
        self.localCouchMonitor.deleteReplicatorDocs()
        
        for rp in self.replicatorDocs:
            self.localCouchMonitor.couchServer.replicate(
                                           rp['source'], rp['target'], filter = rp['filter'], 
                                           query_params = rp.get('query_params', False),
                                           continuous = True, useReplicator = True)
        # First cicle need to be skipped since document is not updated that fast
        self.skipReplicationCheck = True
                     
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        
        if hasattr(self.config, "Tier0Feeder"):
            self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL, 
                                                       appName= "WMStatsAgent")
        else:
            self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)
        
        self.localCouchMonitor = CouchMonitor(self.config.JobStateMachine.couchurl)
        self.setUpCouchDBReplication()

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
            
        except Exception as ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
    
     
    def collectCouchDBInfo(self):
        
        couchInfo = {'status': 'ok', 'error_message': ""}
        
        if self.skipReplicationCheck:
            # skipping the check this round set if False so it can be checked next round.
            self.skipReplicationCheck = False
            return couchInfo
        
        msg = ""
        for rp in self.replicatorDocs:
            cInfo = self.localCouchMonitor.checkCouchServerStatus(rp['source'], 
                                                        rp['target'], checkUpdateSeq = False)
            if cInfo['status'] != 'ok':
                couchInfo['status'] = 'error'
        
        couchInfo['error_message'] = msg
        return couchInfo
        
    def collectAgentInfo(self):
        
        agentInfo = self.wmagentDB.getComponentStatus(self.config)
        agentInfo.update(self.agentInfo)
        
        if isDrainMode(self.config):
            logging.info("Agent is in DrainMode")
            agentInfo['drain_mode'] = True
            agentInfo['status'] = "warning"
        
        else:
            agentInfo['drain_mode'] = False
        
        couchInfo = self.collectCouchDBInfo()
        
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
            if ('data_error' in agentInfo and agentInfo['data_error'] != 'ok') or \
               ('couch_process_warning' in agentInfo and agentInfo['couch_process_warning'] != 0):
                agentInfo['status'] = "error"

        return agentInfo

    def uploadAgentInfoToCentralWMStats(self, agentInfo, uploadTime):
        #direct data upload to the remote to prevent data conflict when agent is cleaned up and redeployed
        agentDocs = convertToAgentCouchDoc(agentInfo, self.config.ACDC, uploadTime)
        self.centralWMStatsCouchDB.updateAgentInfo(agentDocs)

