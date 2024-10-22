"""
Perform general agent monitoring, like:
 1. Status of the agent processes
 2. Status of the agent threads
 3. Couchdb replication status (and status of its database)
 4. Disk usage status
"""
from future.utils import viewitems

import os
import time
import json
import logging
import threading
from pprint import pformat
from Utils.Timers import timeFunction
from Utils.Utilities import numberCouchProcess
from WMComponent.AgentStatusWatcher.DrainStatusPoller import DrainStatusPoller
from WMComponent.AnalyticsDataCollector.DataCollectAPI import WMAgentDBData, initAgentInfo
from WMCore.Credential.Proxy import Proxy
from WMCore.Database.CMSCouch import CouchMonitor
from WMCore.Lexicon import sanitizeURL
from WMCore.Services.ReqMgrAux.ReqMgrAux import isDrainMode, listDiskUsageOverThreshold
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import getGlobalSiteStatusSummary
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

# CMSMonitoring modules
from CMSMonitoring.StompAMQ7 import StompAMQ7 as StompAMQ


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

        proxyArgs = {'logger': logging.getLogger(), 'cleanEnvironment': True}
        self.proxy = Proxy(proxyArgs)
        self.proxyFile = self.proxy.getProxyFilename()  # X509_USER_PROXY
        self.userCertFile = self.proxy.getUserCertFilename()  # X509_USER_CERT
        # credential lifetime warning/error thresholds, in days
        self.credThresholds = {'proxy': {'error': 3, 'warning': 5},
                               'certificate': {'error': 10, 'warning': 20}}

        # Monitoring setup
        self.userAMQ = getattr(config.AgentStatusWatcher, "userAMQ", None)
        self.passAMQ = getattr(config.AgentStatusWatcher, "passAMQ", None)
        self.postToAMQ = getattr(config.AgentStatusWatcher, "enableAMQ", False)
        self.topicAMQ = getattr(config.AgentStatusWatcher, "topicAMQ", None)
        self.hostPortAMQ = getattr(config.AgentStatusWatcher, "hostPortAMQ", [('cms-mb.cern.ch', 61313)])

        # Load CouchDB replication filters
        # see: https://github.com/dmwm/CMSKubernetes/blob/69f0a02a52101ef/docker/pypi/wmagent/Dockerfile#L34
        jsonDir = os.environ.get("WMA_DEPLOY_DIR", "/usr/local")
        replicationFile = os.path.join(jsonDir, "etc/replication_selector.json")
        if os.path.exists(replicationFile):
            with open(replicationFile, 'r') as fd:
                self.replicationDict = json.load(fd)
        else:
            raise RuntimeError(f"Could not find CouchDB replication JSON file at: {replicationFile}")

        # T0 doesn't have WorkQueue, so some monitoring/replication code has to be skipped here
        if hasattr(self.config, "Tier0Feeder"):
            self.isT0agent = True
            self.producer = "tier0wmagent"
        else:
            self.isT0agent = False
            self.producer = "wmagent"
            localWQUrl = config.AnalyticsDataCollector.localQueueURL
            self.workqueueDS = WorkQueueDS(localWQUrl)

    def setUpCouchDBReplication(self):
        """
        This method will delete the current replication documents and
        fresh new ones will be created.
        :return: None
        """
        # delete old replicator docs before setting up fresh ones
        resp = self.localCouchMonitor.deleteReplicatorDocs()
        logging.info("Deleted old replication documents and the response was: %s", resp)

        self.replicatorDocs = []
        # set up common replication code
        self.replicatorDocs.append({'source': self.config.JobStateMachine.jobSummaryDBName,
                                    'target': self.config.General.centralWMStatsURL,
                                    'selector': self.replicationDict["WMStatsAgent/repfilter"]})

        if self.isT0agent:
            # Tier0 specific replication
            self.replicatorDocs.append({'source': self.config.Tier0Feeder.requestDBName,
                                        'target': self.config.AnalyticsDataCollector.centralRequestDBURL,
                                        'selector': self.replicationDict["T0Request/repfilter"]})
        else:
            # Production specific workqueue replication
            parentQueueUrl = self.config.WorkQueueManager.queueParams["ParentQueueCouchUrl"]
            childQueueUrl = self.config.WorkQueueManager.queueParams["QueueURL"]
            localQInboxURL = "%s_inbox" % self.config.AnalyticsDataCollector.localQueueURL
            # Update the selector filter
            workqueueEscapedKey = "WMCore\.WorkQueue\.DataStructs\.WorkQueueElement\.WorkQueueElement"
            self.replicationDict['WorkQueue/queueFilter'][workqueueEscapedKey]["ChildQueueUrl"] = childQueueUrl
            self.replicatorDocs.append({'source': parentQueueUrl,
                                        'target': localQInboxURL,
                                        'selector': self.replicationDict['WorkQueue/queueFilter']})
            self.replicatorDocs.append({'source': localQInboxURL,
                                        'target': parentQueueUrl,
                                        'selector': self.replicationDict['WorkQueue/queueFilter']})

        for rp in self.replicatorDocs:
            # ensure credentials don't get exposed in the logs
            msg = f"Creating continuous replication for source: {sanitizeURL(rp['source'])['url']}, "
            msg += f"target: {sanitizeURL(rp['target'])['url']} and selector filter: {rp['selector']}"
            logging.info(msg)
            resp = self.localCouchMonitor.couchServer.replicate(rp['source'], rp['target'],
                                                                continuous=True,
                                                                selector=rp.get('selector', False))
            logging.info(".. response for the replication document creation was: %s", resp)

    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)

        self.centralWMStatsCouchDB = WMStatsWriter(self.config.General.centralWMStatsURL)

        self.localCouchMonitor = CouchMonitor(self.config.JobStateMachine.couchurl)
        self.setUpCouchDBReplication()

    @timeFunction
    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            agentInfo = self.collectAgentInfo()
            self.checkCredLifetime(agentInfo, "proxy")
            self.checkCredLifetime(agentInfo, "certificate")

            timeSpent, wmbsInfo, _ = self.collectWMBSInfo()
            wmbsInfo['total_query_time'] = int(timeSpent)
            agentInfo["WMBS_INFO"] = wmbsInfo
            logging.info("WMBS data collected in: %d secs", timeSpent)

            if not self.isT0agent:
                timeSpent, localWQInfo, _ = self.collectWorkQueueInfo()
                localWQInfo['total_query_time'] = int(timeSpent)
                agentInfo["LocalWQ_INFO"] = localWQInfo
                logging.info("Local WorkQueue data collected in: %d secs", timeSpent)

            self.uploadAgentInfoToCentralWMStats(agentInfo)

            self.buildMonITDocs(agentInfo)

        except Exception as ex:
            logging.exception("Error occurred, will retry later.\nDetails: %s", str(ex))

    @timeFunction
    def collectWorkQueueInfo(self):
        """
        Collect information from local workqueue database
        :return:
        """
        results = {}
        wqStates = ['Available', 'Acquired']

        results['workByStatus'] = self.workqueueDS.getJobsByStatus()
        results['workByStatusAndPriority'] = self.workqueueDS.getJobsByStatusAndPriority()

        elements = self.workqueueDS.getElementsByStatus(wqStates)
        uniSites, posSites = getGlobalSiteStatusSummary(elements, status=wqStates, dataLocality=True)
        results['uniqueJobsPerSite'] = uniSites
        results['possibleJobsPerSite'] = posSites

        return results

    def checkCouchStatus(self):
        """
        This method checks whether CouchDB is running properly and it also
        verifies whether all the replication tasks are progressing as expected
        :return: a dictionary with the status for CouchServer
        """
        couchInfo = {'name': 'CouchServer', 'status': 'ok', 'error_message': ""}

        cInfo = self.localCouchMonitor.checkCouchReplications(self.replicatorDocs)
        couchInfo.update(cInfo)
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

        agentInfo['disk_warning'] = listDiskUsageOverThreshold(self.config, updateDB=True)

        if isDrainMode(self.config):
            logging.info("Agent is in DrainMode")
            agentInfo['drain_mode'] = True
            agentInfo['drain_stats'] = DrainStatusPoller.getDrainInfo()
        else:
            agentInfo['drain_mode'] = False

        couchInfo = self.checkCouchStatus()
        if couchInfo['status'] != 'ok':
            agentInfo['down_components'].append(couchInfo['name'])
            agentInfo['status'] = couchInfo['status']
            agentInfo['down_component_detail'].append(couchInfo)

        # Couch process warning
        couchProc = numberCouchProcess()
        logging.info("CouchDB is running with %d processes", couchProc)
        couchProcessThreshold = self.config.AnalyticsDataCollector.couchProcessThreshold
        if couchProc >= couchProcessThreshold:
            agentInfo['couch_process_warning'] = couchProc
        else:
            agentInfo['couch_process_warning'] = 0

        # Change status if there is data_error, couch process maxed out or disk full problems.
        if agentInfo['status'] == 'ok' and (agentInfo['drain_mode'] or agentInfo['disk_warning']):
            agentInfo['status'] = "warning"

        if agentInfo['status'] == 'ok' or agentInfo['status'] == 'warning':
            if agentInfo.get('data_error', 'ok') != 'ok' or agentInfo.get('couch_process_warning', 0):
                agentInfo['status'] = "error"

        logging.info("List of agent components down: %s", agentInfo['down_components'])

        return agentInfo

    def uploadAgentInfoToCentralWMStats(self, agentInfo):
        """
        Add some required fields to the document before it can get uploaded
        to WMStats.
        :param agentInfo: dict with agent stats to be posted to couchdb
        """
        agentInfo['_id'] = agentInfo["agent_url"]
        agentInfo['timestamp'] = int(time.time())
        agentInfo['type'] = "agent_info"
        # directly upload to the remote to prevent data conflict when agent is cleaned up and redeployed
        try:
            self.centralWMStatsCouchDB.updateAgentInfo(agentInfo,
                                                       propertiesToKeep=["data_last_update", "data_error"])
        except Exception as e:
            logging.error("Failed to upload agent statistics to WMStats. Error: %s", str(e))

    @timeFunction
    def collectWMBSInfo(self):
        """
        Fetches WMBS job information.
        In addition to WMBS, also collects RunJob info from BossAir
        :return: dict with the number of jobs in each status
        """
        logging.info("Getting wmbs job info ...")
        results = {}

        # first retrieve the site thresholds
        results['thresholds'] = self.wmagentDB.getJobSlotInfo()
        logging.debug("Running and pending site thresholds: %s", results['thresholds'])

        # now fetch the amount of jobs in each state and the amount of created
        # jobs grouped by task
        results.update(self.wmagentDB.getAgentMonitoring())

        logging.debug("Total number of jobs in WMBS sorted by status: %s", results['wmbsCountByState'])
        logging.debug("Total number of 'created' jobs in WMBS sorted by type: %s", results['wmbsCreatedTypeCount'])
        logging.debug("Total number of 'executing' jobs in WMBS sorted by type: %s", results['wmbsExecutingTypeCount'])

        logging.debug("Total number of active jobs in BossAir sorted by status: %s", results['activeRunJobByStatus'])
        logging.debug("Total number of complete jobs in BossAir sorted by status: %s",
                      results['completeRunJobByStatus'])

        logging.debug("Available slots thresholds to pull work from GQ to LQ: %s", results['thresholdsGQ2LQ'])
        logging.debug("List of jobs pending for each site, sorted by priority: %s", results['sitePendCountByPrio'])

        return results

    def checkCredLifetime(self, agInfo, credType):
        """
        Check the credential lifetime. Usually X509_USER_PROXY or X509_USER_CERT
        and raise either a warning or an error if the proxy validity is about to expire.
        :param agInfo: dictionary with plenty of agent monitoring information in place.
        :param credType: credential type, can be: "proxy" or "certificate"
        :return: same dictionary object plus additional keys/values if needed.
        """
        if credType == "proxy":
            credFile = self.proxyFile
            secsLeft = self.proxy.getTimeLeft(proxy=credFile)
        elif credType == "certificate":
            credFile = self.userCertFile
            secsLeft = self.proxy.getUserCertTimeLeft(openSSL=True)
        else:
            logging.error("Unknown credential type. Available options are: [proxy, certificate]")
            return

        logging.debug("%s '%s' lifetime is %d seconds", credType, credFile, secsLeft)

        daysLeft = secsLeft / (60 * 60 * 24)

        if daysLeft <= self.credThresholds[credType]['error']:
            credWarning = True
            agInfo['status'] = "error"
        elif daysLeft <= self.credThresholds[credType]['warning']:
            credWarning = True
            if agInfo['status'] == "ok":
                agInfo['status'] = "warning"
        else:
            credWarning = False

        if credWarning:
            warnMsg = "Agent %s '%s' must be renewed ASAP. " % (credType, credFile)
            warnMsg += "Its time left is: %.2f hours;" % (secsLeft / 3600.)
            agInfo['proxy_warning'] = agInfo.get('proxy_warning', "") + warnMsg
            logging.warning(warnMsg)

        return

    def buildMonITDocs(self, dataStats):
        """
        Convert agent statistics into MonIT-friendly documents to be posted
        to AMQ/ES. It creates 5 different type of documents:
         * priority information
         * site information
         * work information
         * agent information
         * agent health information
        Note that the internal methods are popping some metrics out of dataStats
        """
        if not self.postToAMQ:
            return

        logging.info("Preparing documents to be posted to AMQ/MonIT..")
        allDocs = self._buildMonITPrioDocs(dataStats)
        allDocs.extend(self._buildMonITSitesDocs(dataStats))
        allDocs.extend(self._buildMonITWorkDocs(dataStats))
        allDocs.extend(self._buildMonITWMBSDocs(dataStats))
        allDocs.extend(self._buildMonITAgentDocs(dataStats))
        allDocs.extend(self._buildMonITHealthDocs(dataStats))
        allDocs.extend(self._buildMonITSummaryDocs(dataStats))

        # and finally post them all to AMQ
        logging.info("Found %d documents to post to AMQ", len(allDocs))
        self.uploadToAMQ(allDocs, dataStats['agent_url'], dataStats['timestamp'])


    def _buildMonITPrioDocs(self, dataStats):
        """
        Uses the `sitePendCountByPrio` metric in order to build documents
        reporting the site name, job priority and amount of jobs within that
        priority.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_prio_info MonIT docs
        """
        docType = "wma_prio_info"
        prioDocs = []
        sitePendCountByPrio = dataStats['WMBS_INFO'].pop('sitePendCountByPrio', [])

        for site, item in viewitems(sitePendCountByPrio):
            # it seems sites with no jobs are also always here as "Sitename": {0: 0}
            if list(item) == [0]:
                continue
            for prio, jobs in viewitems(item):
                prioDoc = {}
                prioDoc['site_name'] = site
                prioDoc['type'] = docType
                prioDoc['priority'] = prio
                prioDoc['job_count'] = jobs
                prioDocs.append(prioDoc)
        return prioDocs

    def _buildMonITSitesDocs(self, dataStats):
        """
        Uses the site thresholds and job information for each site in order
        to build a `site_info` document type for MonIT.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_site_info MonIT docs
        """
        docType = "wma_site_info"
        siteDocs = []
        thresholds = dataStats['WMBS_INFO'].pop('thresholds', {})
        thresholdsGQ2LQ = dataStats['WMBS_INFO'].pop('thresholdsGQ2LQ', {})
        if self.isT0agent:
            possibleJobsPerSite = {}
            uniqueJobsPerSite = {}
        else:
            possibleJobsPerSite = dataStats['LocalWQ_INFO'].pop('possibleJobsPerSite', {})
            uniqueJobsPerSite = dataStats['LocalWQ_INFO'].pop('uniqueJobsPerSite', {})

        for site in sorted(thresholds):
            siteDoc = {}
            siteDoc['site_name'] = site
            siteDoc['type'] = docType
            siteDoc['thresholds'] = thresholds[site]
            siteDoc['state'] = siteDoc['thresholds'].pop('state', 'Unknown')
            siteDoc['thresholdsGQ2LQ'] = thresholdsGQ2LQ.get(site, 0)

            for status in possibleJobsPerSite:
                # make sure these keys are always present in the documents
                jobKey = "possible_%s_jobs" % status.lower()
                elemKey = "num_%s_elem" % status.lower()
                uniJobKey = "unique_%s_jobs" % status.lower()
                siteDoc[jobKey], siteDoc[elemKey], siteDoc[uniJobKey] = 0, 0, 0
                if site in possibleJobsPerSite[status]:
                    siteDoc[jobKey] = possibleJobsPerSite[status][site]['sum_jobs']
                    siteDoc[elemKey] = possibleJobsPerSite[status][site]['num_elem']
                if site in uniqueJobsPerSite[status]:
                    siteDoc[uniJobKey] = uniqueJobsPerSite[status][site]['sum_jobs']

            siteDocs.append(siteDoc)

        return siteDocs

    def _buildMonITWorkDocs(self, dataStats):
        """
        Uses the local workqueue information order by WQE status and build
        statistics for the workload in terms of workqueue elements and top
        level jobs.
        Using the WMBS data, also builds documents to show the amount of
        work in 'created' and 'executing' WMBS status.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_work_info MonIT docs
        """
        workDocs = []
        if self.isT0agent:
            return workDocs

        docType = "wma_work_info"
        workByStatus = dataStats['LocalWQ_INFO'].pop('workByStatus', {})
        for status, info in viewitems(workByStatus):
            workDoc = {}
            workDoc['type'] = docType
            workDoc['status'] = status
            workDoc['num_elem'] = info.get('num_elem', 0)
            workDoc['sum_jobs'] = info.get('sum_jobs', 0)
            workDocs.append(workDoc)

        return workDocs

    def _buildMonITWMBSDocs(self, dataStats):
        """
        Using the WMBS data, builds documents to show the amount of work in
        'created' and 'executing' WMBS status.
        It also builds a document for every single wmbs_status in the database.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_wmbs_info and wma_wmbs_state_info docs
        """
        docType = "wma_wmbs_info"
        wmbsDocs = []
        wmbsCreatedTypeCount = dataStats['WMBS_INFO'].pop('wmbsCreatedTypeCount', {})
        wmbsExecutingTypeCount = dataStats['WMBS_INFO'].pop('wmbsExecutingTypeCount', {})
        for jobType in wmbsCreatedTypeCount:
            wmbsDoc = {}
            wmbsDoc['type'] = docType
            wmbsDoc['job_type'] = jobType
            wmbsDoc['created_jobs'] = wmbsCreatedTypeCount[jobType]
            wmbsDoc['executing_jobs'] = wmbsExecutingTypeCount[jobType]
            wmbsDocs.append(wmbsDoc)

        docType = "wma_wmbs_state_info"
        wmbsCountByState = dataStats['WMBS_INFO'].pop('wmbsCountByState', {})
        for wmbsStatus in wmbsCountByState:
            wmbsDoc = {}
            wmbsDoc['type'] = docType
            wmbsDoc['wmbs_status'] = wmbsStatus
            wmbsDoc['num_jobs'] = wmbsCountByState[wmbsStatus]
            wmbsDocs.append(wmbsDoc)

        return wmbsDocs

    def _buildMonITAgentDocs(self, dataStats):
        """
        Uses the BossAir and WMBS table information in order to build a
        view of amount of jobs in different statuses.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_agent_info MonIT docs
        """
        docType = "wma_agent_info"
        agentDocs = []
        activeRunJobByStatus = dataStats['WMBS_INFO'].pop('activeRunJobByStatus', {})
        completeRunJobByStatus = dataStats['WMBS_INFO'].pop('completeRunJobByStatus', {})
        for schedStatus in activeRunJobByStatus:
            agentDoc = {}
            agentDoc['type'] = docType
            agentDoc['schedd_status'] = schedStatus
            agentDoc['active_jobs'] = activeRunJobByStatus[schedStatus]
            agentDoc['completed_jobs'] = completeRunJobByStatus[schedStatus]
            agentDocs.append(agentDoc)

        return agentDocs

    def _buildMonITHealthDocs(self, dataStats):
        """
        Creates documents with specific agent information, status of
        each component and worker thread (similar to what is shown in
        wmstats) and also some very basic performance numbers.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_health_info MonIT docs
        """
        docType = "wma_health_info"
        healthDocs = []
        workersStatus = dataStats.pop('workers', {})
        for worker in workersStatus:
            healthDoc = {}
            healthDoc['type'] = docType
            healthDoc['worker_name'] = worker['name']
            healthDoc['worker_state'] = worker['state']
            healthDoc['worker_poll'] = worker['poll_interval']
            healthDoc['worker_last_hb'] = worker['last_updated']
            healthDoc['worker_cycle_time'] = worker['cycle_time']
            healthDocs.append(healthDoc)

        return healthDocs

    def _buildMonITSummaryDocs(self, dataStats):
        """
        Creates a document with the very basic agent info used
        in the wmstats monitoring tab.
        :param dataStats: dictionary with metrics previously posted to WMStats
        :return: list of dictionaries with the wma_health_info MonIT docs
        """
        docType = "wma_summary_info"
        summaryDocs = []
        summaryDoc = {}
        summaryDoc['type'] = docType
        summaryDoc['agent_team'] = dataStats['agent_team']
        summaryDoc['agent_version'] = dataStats['agent_version']
        summaryDoc['agent_status'] = dataStats['status']
        if not self.isT0agent:
            summaryDoc['wq_query_time'] = dataStats['LocalWQ_INFO']['total_query_time']
        summaryDoc['wmbs_query_time'] = dataStats['WMBS_INFO']['total_query_time']
        summaryDoc['drain_mode'] = dataStats['drain_mode']
        summaryDoc['down_components'] = dataStats['down_components']
        summaryDocs.append(summaryDoc)
        return summaryDocs

    def uploadToAMQ(self, docs, agentUrl, timeS):
        """
        _uploadToAMQ_

        Sends data to AMQ, which ends up in the MonIT infrastructure.
        :param docs: list of documents/dicts to be posted
        """
        if not docs:
            logging.info("There are no documents to send to AMQ")
            return
        # add mandatory information for every single document
        for doc in docs:
            doc['agent_url'] = agentUrl

        docType = "cms_%s_info" % self.producer
        notifications = []

        logging.debug("Sending the following data to AMQ %s", pformat(docs))
        try:
            stompSvc = StompAMQ(username=self.userAMQ,
                                password=self.passAMQ,
                                producer=self.producer,
                                topic=self.topicAMQ,
                                validation_schema=None,
                                host_and_ports=self.hostPortAMQ,
                                logger=logging)

            for doc in docs:
                singleNotif, _, _ = stompSvc.make_notification(payload=doc, doc_type=docType,
                                                               ts=timeS, data_subfield="payload")
                notifications.append(singleNotif)

            failures = stompSvc.send(notifications)
            msg = "%i out of %i documents successfully sent to AMQ" % (len(notifications) - len(failures),
                                                                       len(notifications))
            logging.info(msg)
        except Exception as ex:
            logging.exception("Failed to send data to StompAMQ. Error %s", str(ex))

        return
