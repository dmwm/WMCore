"""
Perform cleanup actions
"""
import threading
import logging
import time

from Utils.Timers import timeFunction
from WMCore.WMFactory import WMFactory
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Database.CMSCouch import CouchMonitor
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import (LocalCouchDBData,
                                            WMAgentDBData, combineAnalyticsData,
                                            convertToRequestCouchDoc, initAgentInfo)

from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil


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
        if not hasattr(self.config, "Tier0Feeder"):
            self.localQueue = WorkQueueService(self.config.AnalyticsDataCollector.localQueueURL)

        # set the connection for local couchDB call
        self.localCouchDB = LocalCouchDBData(self.config.AnalyticsDataCollector.localCouchURL,
                                             self.config.JobStateMachine.summaryStatsDBName,
                                             self.summaryLevel)

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
        # set the connection for local couchDB call
        self.localSummaryCouchDB = WMStatsWriter(self.config.AnalyticsDataCollector.localWMStatsURL,
                                                 appName="WMStatsAgent")

        # use local db for tier0
        if hasattr(self.config, "Tier0Feeder"):
            centralRequestCouchDBURL = self.config.AnalyticsDataCollector.localT0RequestDBURL
        else:
            centralRequestCouchDBURL = self.config.AnalyticsDataCollector.centralRequestDBURL

        self.centralRequestCouchDB = RequestDBWriter(centralRequestCouchDBURL,
                                                     couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)
        self.centralWMStatsCouchDB = WMStatsWriter(self.config.General.centralWMStatsURL)

        #TODO: change the config to hold couch url
        self.localCouchServer = CouchMonitor(self.config.JobStateMachine.couchurl)

        self.dbsBufferUtil = DBSBufferUtil()

        if self.pluginName is not None:
            pluginFactory = WMFactory("plugins", "WMComponent.AnalyticsDataCollector.Plugins")
            self.plugin = pluginFactory.loadObject(classname=self.pluginName)

    @timeFunction
    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            # jobs per request info
            logging.info("Getting Job Couch Data ...")
            jobInfoFromCouch = self.localCouchDB.getJobSummaryByWorkflowAndSite()

            # fwjr per request info
            logging.info("Getting FWJRJob Couch Data ...")

            fwjrInfoFromCouch = self.localCouchDB.getJobPerformanceByTaskAndSiteFromSummaryDB()
            skippedInfoFromCouch = self.localCouchDB.getSkippedFilesSummaryByWorkflow()

            logging.info("Getting Batch Job Data ...")
            batchJobInfo = self.wmagentDB.getBatchJobInfo()

            logging.info("Getting Finished Task Data ...")
            finishedTasks = self.wmagentDB.getFinishedSubscriptionByTask()

            logging.info("Getting DBS PhEDEx upload status ...")
            completedWfs = self.dbsBufferUtil.getPhEDExDBSStatusForCompletedWorkflows(summary=True)

            # get the data from local workqueue:
            # request name, input dataset, inWMBS, inQueue
            logging.info("Getting Local Queue Data ...")
            localQInfo = {}
            if not hasattr(self.config, "Tier0Feeder"):
                localQInfo = self.localQueue.getAnalyticsData()
            else:
                logging.debug("Tier-0 instance, not checking WorkQueue")

            # combine all the data from 3 sources
            logging.info("""Combining data from
                                   Job Couch(%s),
                                   FWJR(%s),
                                   WorkflowsWithSkippedFile(%s),
                                   Batch Job(%s),
                                   Finished Tasks(%s),
                                   Local Queue(%s)
                                   Completed workflows(%s)..  ...""",
                         len(jobInfoFromCouch), len(fwjrInfoFromCouch), len(skippedInfoFromCouch),
                         len(batchJobInfo), len(finishedTasks), len(localQInfo), len(completedWfs))

            tempCombinedData = combineAnalyticsData(jobInfoFromCouch, batchJobInfo)
            tempCombinedData2 = combineAnalyticsData(tempCombinedData, localQInfo)
            combinedRequests = combineAnalyticsData(tempCombinedData2, completedWfs)

            # set the uploadTime - should be the same for all docs
            uploadTime = int(time.time())

            logging.info("%s requests Data combined,\n uploading request data...", len(combinedRequests))
            requestDocs = convertToRequestCouchDoc(combinedRequests, fwjrInfoFromCouch, finishedTasks,
                                                   skippedInfoFromCouch, self.agentInfo,
                                                   uploadTime, self.summaryLevel)

            if self.plugin != None:
                self.plugin(requestDocs, self.localSummaryCouchDB, self.centralRequestCouchDB)

            existingDocs = self.centralWMStatsCouchDB.getAllAgentRequestRevByID(self.agentInfo["agent_url"])
            self.centralWMStatsCouchDB.bulkUpdateData(requestDocs, existingDocs)

            logging.info("Request data upload success\n %s request, \nsleep for next cycle", len(requestDocs))

            self.centralWMStatsCouchDB.updateAgentInfoInPlace(self.agentInfo["agent_url"],
                                                              {"data_last_update": uploadTime, "data_error": "ok"})

        except Exception as ex:
            msg = str(ex)
            logging.exception("Error occurred, will retry later: %s", msg)

            try:
                self.centralWMStatsCouchDB.updateAgentInfoInPlace(self.agentInfo["agent_url"], {"data_error": msg})
            except:
                logging.error("upload Agent Info to central couch failed")


