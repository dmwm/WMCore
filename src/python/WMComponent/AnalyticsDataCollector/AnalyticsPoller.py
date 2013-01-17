"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
import traceback
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import LocalCouchDBData, \
     WMAgentDBData, combineAnalyticsData, convertToRequestCouchDoc, \
     convertToAgentCouchDoc
from WMCore.WMFactory import WMFactory

class AnalyticsPoller(BaseWorkerThread):
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
        self.agentInfo = {}
        self.agentInfo['agent_team'] = config.Agent.teamName
        self.agentInfo['agent'] = config.Agent.agentName
        # temporarly add port for the split test
        self.agentInfo['agent_url'] = ("%s:%s" % (config.Agent.hostName, config.WMBSService.Webtools.port))
        # need to get campaign, user, owner info
        self.agentDocID = "agent+hostname"
        self.summaryLevel = (config.AnalyticsDataCollector.summaryLevel).lower()
        self.pluginName = getattr(config.AnalyticsDataCollector, "pluginName", None)
        self.plugin = None
        
            
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """

        #
        self.localQueue = WorkQueueService(self.config.AnalyticsDataCollector.localQueueURL)

        # set the connection for local couchDB call
        self.localCouchDB = LocalCouchDBData(self.config.AnalyticsDataCollector.localCouchURL, self.summaryLevel)

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        # set the connection for local couchDB call
        self.localSummaryCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL)
        logging.info("Setting the replication to central monitor ...")
        self.localSummaryCouchDB.replicate(self.config.AnalyticsDataCollector.centralWMStatsURL)
       
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)
        
        if self.pluginName != None:
            pluginFactory = WMFactory("plugins", "WMComponent.AnalyticsDataCollector.Plugins")
            self.plugin = pluginFactory.loadObject(classname = self.pluginName)

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            #jobs per request info
            logging.info("Getting Job Couch Data ...")
            jobInfoFromCouch = self.localCouchDB.getJobSummaryByWorkflowAndSite()

            #fwjr per request info
            logging.info("Getting FWJRJob Couch Data ...")

            #fwjrInfoFromCouch = self.localCouchDB.getEventSummaryByWorkflow()
            fwjrInfoFromCouch = self.localCouchDB.getJobPerformanceByTaskAndSite()
            
            logging.info("Getting Batch Job Data ...")
            batchJobInfo = self.wmagentDB.getBatchJobInfo()
            
            logging.info("Getting Finished Task Data ...")
            finishedTasks = self.wmagentDB.getFinishedSubscriptionByTask()

            # get the data from local workqueue:
            # request name, input dataset, inWMBS, inQueue
            logging.info("Getting Local Queue Data ...")
            localQInfo = self.localQueue.getAnalyticsData()

            # combine all the data from 3 sources
            logging.info("""Combining data from
                                   Job Couch(%s),
                                   FWJR(%s), 
                                   Batch Job(%s),
                                   Finished Tasks(%s),
                                   Local Queue(%s)  ...""" 
                    % (len(jobInfoFromCouch), len(fwjrInfoFromCouch), len(batchJobInfo), len(finishedTasks), len(localQInfo)))

            tempCombinedData = combineAnalyticsData(jobInfoFromCouch, batchJobInfo)
            combinedRequests = combineAnalyticsData(tempCombinedData, localQInfo)

            #set the uploadTime - should be the same for all docs
            uploadTime = int(time.time())
            logging.info("%s requests Data combined,\n uploading request data..." % len(combinedRequests))
            requestDocs = convertToRequestCouchDoc(combinedRequests, fwjrInfoFromCouch, finishedTasks,
                                                   self.agentInfo, uploadTime, self.summaryLevel)


            if self.plugin != None:
                self.plugin(requestDocs, self.localSummaryCouchDB, self.centralWMStatsCouchDB)

            self.localSummaryCouchDB.uploadData(requestDocs)
            logging.info("Request data upload success\n %s request \n uploading agent data" % len(requestDocs))

            #TODO: agent info (need to include job Slots for the sites)
            agentInfo = self.wmagentDB.getHeartbeatWarning()
            agentInfo.update(self.agentInfo)

            couchInfo = self.checkLocalCouchServerStatus()
            if (couchInfo['status'] != 'ok'):
                agentInfo['down_components'].append("CouchServer")
                agentInfo['status'] = couchInfo['status']
                couchInfo['name'] = "CouchServer"
                agentInfo['down_component_detail'].append(couchInfo)

            #direct data upload to the remote to prevent data conflict when agent is cleaned up and redeployed
            agentDocs = convertToAgentCouchDoc(agentInfo, self.config.ACDC, uploadTime)
            self.centralWMStatsCouchDB.updateAgentInfo(agentDocs)
            logging.info("Agent data direct upload success\n %s request" % len(agentDocs))

        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
    
    
    def checkLocalCouchServerStatus(self):
        localCouchServer = self.localSummaryCouchDB.getServerInstance()
        try:
            status = localCouchServer.status()
            replicationError = True
            for activeStatus in status['active_tasks']:
                if activeStatus["type"] == "Replication":
                    if "wmagent_summary" in activeStatus["task"]:
                        replicationError = False
                        break
            if replicationError:
                return {'status':'error', 'error_message': "replication stopped"}
            else:
                return {'status': 'ok'}
        except Exception, ex:
            return {'status':'down', 'error_message': str(ex)}

