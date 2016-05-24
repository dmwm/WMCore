"""
Perform general agent monitoring, like:
 1. Status of the agent processes
 2. Status of the agent threads
 3. Couchdb replication status (and status of its database)
 4. Disk usage status
"""
__all__ = []



import threading
import logging
import time
import traceback
import json
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
        self.summaryLevel = config.AnalyticsDataCollector.summaryLevel
        self.jsonFile = config.AgentStatusWatcher.jsonFile
        # counter for deep agent monitoring. Every 15min (3 cycles of the component)
        self.monitorCounter = 0
        self.monitorInterval = getattr(config.AgentStatusWatcher, 'monitorPollInterval', 3)
    
    def setUpCouchDBReplication(self):
        
        self.replicatorDocs = []
        # set up common replication code
        wmstatsSource = self.config.JobStateMachine.jobSummaryDBName
        wmstatsTarget = self.config.AnalyticsDataCollector.centralWMStatsURL
        
        self.replicatorDocs.append({'source': wmstatsSource, 'target': wmstatsTarget, 
                                    'filter':  "WMStatsAgent/repfilter"})
        #TODO: tier0 specific code - need to make it generic 
        if hasattr(self.config, "Tier0Feeder"):
            t0Source = self.config.Tier0Feeder.requestDBName
            t0Target = self.config.AnalyticsDataCollector.centralRequestDBURL
            self.replicatorDocs.append({'source': t0Source, 'target': t0Target, 
                                        'filter': "T0Request/repfilter"})
        else: # set up workqueue replication
            wqfilter = 'WorkQueue/queueFilter'
            parentQURL = self.config.WorkQueueManager.queueParams["ParentQueueCouchUrl"]
            childURL = self.config.WorkQueueManager.queueParams["QueueURL"]
            query_params = {'childUrl' : childURL, 'parentUrl' : sanitizeURL(parentQURL)['url']}
            localQInboxURL = "%s_inbox" % self.config.AnalyticsDataCollector.localQueueURL
            self.replicatorDocs.append({'source': sanitizeURL(parentQURL)['url'], 'target': localQInboxURL, 
                                        'filter': wqfilter, 'query_params': query_params})       
            self.replicatorDocs.append({'source': sanitizeURL(localQInboxURL)['url'], 'target': parentQURL, 
                                        'filter': wqfilter, 'query_params': query_params})

        # delete old replicator docs before setting up
        self.localCouchMonitor.deleteReplicatorDocs()
        
        for rp in self.replicatorDocs:
            self.localCouchMonitor.couchServer.replicate(
                                           rp['source'], rp['target'], filter = rp['filter'], 
                                           query_params = rp.get('query_params', False),
                                           continuous = True)
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
        
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)
        
        self.localCouchMonitor = CouchMonitor(self.config.JobStateMachine.couchurl)
        self.setUpCouchDBReplication()

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            agentInfo = self.collectAgentInfo()
            #set the uploadTime - should be the same for all docs
            uploadTime = int(time.time())
            self.uploadAgentInfoToCentralWMStats(agentInfo, uploadTime)

            if self.monitorCounter % self.monitorInterval == 0:
                monitoring = self.collectWMBSInfo()
                monitoring['components'] = agentInfo['down_components']
                monitoring['timestamp'] = int(time.time())
                with open(self.jsonFile, 'w') as outFile:
                    json.dump(monitoring, outFile, indent=2)
            self.monitorCounter += 1
        except Exception as ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
    
     
    def collectCouchDBInfo(self):
        
        couchInfo = {'name': 'CouchServer', 'status': 'ok', 'error_message': ""}
        
        if self.skipReplicationCheck:
            # skipping the check this round set if False so it can be checked next round.
            self.skipReplicationCheck = False
            return couchInfo

        for rp in self.replicatorDocs:
            cInfo = self.localCouchMonitor.checkCouchServerStatus(rp['source'], 
                                                        rp['target'], checkUpdateSeq = False)
            if cInfo['status'] != 'ok':
                couchInfo['status'] = 'error'
                couchInfo['error_message'] = cInfo['error_message']
        
        return couchInfo
        
    def collectAgentInfo(self):
        """
        Monitors the general health of the agent, as:
          1. status of the agent processes
          2. status of the agent threads based on the database info
          3. couchdb active tasks and its replications
          4. check the disk usage
          5. check the number of couch processes

        :return: a dict with all the info collected
        """
        logging.info("Getting agent info ...")
        agentInfo = self.wmagentDB.getComponentStatus(self.config)
        agentInfo.update(self.agentInfo)
        
        if isDrainMode(self.config):
            logging.info("Agent is in DrainMode")
            agentInfo['drain_mode'] = True
        else:
            agentInfo['drain_mode'] = False
        
        couchInfo = self.collectCouchDBInfo()
        if couchInfo['status'] != 'ok':
            agentInfo['down_components'].append(couchInfo['name'])
            agentInfo['status'] = couchInfo['status']
            agentInfo['down_component_detail'].append(couchInfo)
        
        
        # Disk space warning   
        diskUseList = diskUse()
        diskUseThreshold = float(self.config.AnalyticsDataCollector.diskUseThreshold)
        agentInfo['disk_warning'] = []
        for disk in diskUseList:
            if float(disk['percent'].strip('%')) >= diskUseThreshold and \
                            disk['mounted'] not in self.config.AnalyticsDataCollector.ignoreDisk:
                agentInfo['disk_warning'].append(disk)
        
        # Couch process warning
        couchProc = numberCouchProcess()
        logging.info("CouchDB is running with %d processes", couchProc)
        couchProcessThreshold = self.config.AnalyticsDataCollector.couchProcessThreshold
        if couchProc >= couchProcessThreshold:
            agentInfo['couch_process_warning'] = couchProc
        else:
            agentInfo['couch_process_warning'] = 0
        
        # This adds the last time and message when data was updated to agentInfo
        lastDataUpload = DataUploadTime.getInfo(self)
        if lastDataUpload['data_last_update']:
            agentInfo['data_last_update'] = lastDataUpload['data_last_update']
        if lastDataUpload['data_error']:
            agentInfo['data_error'] = lastDataUpload['data_error']
        
        # Change status if there is data_error, couch process maxed out or disk full problems.
        if agentInfo['status'] == 'ok' and (agentInfo['drain_mode'] or agentInfo['disk_warning']):
            agentInfo['status'] = "warning"
                
        if agentInfo['status'] == 'ok' or agentInfo['status'] == 'warning':
            if agentInfo.get('data_error', 'ok') != 'ok' or agentInfo.get('couch_process_warning', 0):
                agentInfo['status'] = "error"

        if agentInfo['down_components']:
            logging.info("List of agent components down: %s" % agentInfo['down_components'])

        return agentInfo

    def uploadAgentInfoToCentralWMStats(self, agentInfo, uploadTime):
        #direct data upload to the remote to prevent data conflict when agent is cleaned up and redeployed
        agentDocs = convertToAgentCouchDoc(agentInfo, self.config.ACDC, uploadTime)
        self.centralWMStatsCouchDB.updateAgentInfo(agentDocs)

    def collectWMBSInfo(self):
        """
        Fetches WMBS job information.
        In addition to WMBS, also collects RunJob info from BossAir
        :return: dict with the number of jobs in each status
        """
        results = {}
        logging.info("Getting wmbs job info ...")
        # first retrieve the site thresholds
        results['thresholds'] = self.wmagentDB.getJobSlotInfo()
        logging.info("Running and pending site thresholds: %s", results['thresholds'])

        # now fetch the amount of jobs in each state and the amount of created
        # jobs grouped by task
        results.update(self.wmagentDB.getAgentMonitoring())
        logging.info("Total number of jobs in WMBS sorted by status: %s", results['wmbsCountByState'])
        logging.info("Total number of 'created' jobs in WMBS sorted by type: %s", results['wmbsCreatedTypeCount'])
        logging.info("Total number of 'executing' jobs in WMBS sorted by type: %s", results['wmbsExecutingTypeCount'])

        logging.info("Total number of active jobs in BossAir sorted by status: %s", results['activeRunJobByStatus'])
        logging.info("Total number of complete jobs in BossAir sorted by status: %s", results['completeRunJobByStatus'])

        logging.info("Available slots thresholds to pull work from GQ to LQ: %s", results['thresholdsGQ2LQ'])
        logging.info("List of jobs pending for each site, sorted by priority: %s", results['sitePendCountByPrio'])

        return results
