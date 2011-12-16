"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMComponent.AnalyticsDataCollector.DataCollectAPI import LocalCouchDBData, \
     ReqMonDBData, WMAgentDBData, combineAnalyticsData, convertToStatusSiteFormat


class AnalyticsPoller(BaseWorkerThread):
    """
    Cleans expired items, updates element status.
    """
    def __init__(self, config):
        """
        Initialize config
        """
        BaseWorkerThread.__init__(self)
        # set the workqueue service for REST call
        self.config = config
        self.agentInfo = {}
        self.agentInfo['team'] = config.Agent.teamName
        self.agentInfo['agent'] = config.Agent.agentName
        # need to get campaign, user, owner info
        self.agentDocID = "agent+hostname"

    def setup(self, parameters):
        """
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        
        #
        self.localQueue = WorkQueueService(self.config.AnalyticsDataCollector.localQueueURL)
        
        # set the connection for local couchDB call
        self.localCouchDB = LocalCouchDBData(self.config.AnalyticsDataCollector.localCouchURL)
        
        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(myThread.dbi, myThread.logger)
        # set the connection for local couchDB call
        self.reqMonCouchDB = ReqMonDBData(self.config.AnalyticsDataCollector.wmstatsURL)

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            #jobs per request info
            jobInfoFromCouch = self.localCouchDB.getJobSummaryByWorkflowAndSite()
            logging.debug("CouchData %s" % jobInfoFromCouch)
            batchJobInfo = self.wmagentDB.getBatchJobInfo()
            logging.debug("BatchJobData %s" % batchJobInfo)
            # get the data from local workqueue:
            # request name, input dataset, inWMBS, inQueue
            localQInfo = self.localQueue.getAnalyticsData()
            
            logging.debug("WorkQueueData %s" % localQInfo)
            
            # combine all the data from 3 sources
            tempCombinedData = combineAnalyticsData(jobInfoFromCouch, batchJobInfo)
            combinedRequests = combineAnalyticsData(tempCombinedData, localQInfo)
            requestDocs = []
            uploadTime = int(time.time())
            for request, status in combinedRequests.items():
                doc = {}
                doc.update(self.agentInfo)
                doc['type'] = "request"
                doc['workflow'] = request
                # this will set doc['status'], and doc['sites']
                tempData = convertToStatusSiteFormat(status)
                doc['status'] = tempData['status']
                doc['sites'] = tempData['sites']
                doc['timestamp'] = uploadTime

            self.reqMonCouchDB.uploadData(requestDocs)

            #agent info (include job Slots for the sites)
            #agentInfo = self.wmagentDB.getHeartBeatWarning(self.config.AnalyticsDataCollector.agentURL, 
            #                                               self.getCouchACDCHtmlBase())
            
            #self.reqMonCouchDB.uploadData(agentInfo)

        except Exception, ex:
            #add log
            raise

    def getCouchACDCHtmlBase(self):
        """
        TODO: currently it is hard code to the front page of ACDC
        When there is more information is available, it can be added
        through 
        """

        return '%s/_design/ACDC/collections.html' % (self.config.ReqMonReporter.acdcCouchURL)
