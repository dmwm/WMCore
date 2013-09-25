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
     convertToAgentCouchDoc, isDrainMode, initAgentInfo
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
        # need to get campaign, user, owner info
        self.agentInfo = initAgentInfo(self.config)
        self.summaryLevel = (config.AnalyticsDataCollector.summaryLevel).lower()
        self.pluginName = getattr(config.AnalyticsDataCollector, "pluginName", None)
        self.plugin = None
                    
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """
        # set the connection to local queue
        self.localQueue = WorkQueueService(self.config.AnalyticsDataCollector.localQueueURL)

        # set the connection for local couchDB call
        self.localCouchDB = LocalCouchDBData(self.config.AnalyticsDataCollector.localCouchURL, self.summaryLevel)

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        # set the connection for local couchDB call
        self.localSummaryCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL)
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.centralWMStatsURL)
        #TODO: change the config to hold couch url
        self.localCouchServer = CouchMonitor(self.config.JobStateMachine.couchurl)
        
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
            logging.info("Request data upload success\n %s request, \nsleep for next cycle" % len(requestDocs))

        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
 