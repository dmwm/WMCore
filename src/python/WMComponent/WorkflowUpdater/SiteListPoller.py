#!/usr/bin/env python
"""
File       : SiteListPoller
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: module to update of site lists within a WMAgent
"""

# system modules
import logging
import threading
from pprint import pformat
# WMCore modules
from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
from WMCore.Lexicon import sanitizeURL
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class SiteListPoller(BaseWorkerThread):
    def __init__(self, config):
        """
        Initialize SiteListPoller object
        :param config: a Configuration object with the component configuration
        """
        BaseWorkerThread.__init__(self)
        myThread = threading.currentThread()
        self.logger = myThread.logger

        # get wmstats parameters
        self.wmstatsUrl = getattr(config.WorkflowUpdater, "wmstatsUrl")
        self.wmstatsSrv = WMStatsServer(self.wmstatsUrl, logger=self.logger)
        self.reqStates = getattr(config.WorkflowUpdater, "states", ['running-open', 'acquired'])
        self.wqeStates = getattr(config.WorkflowUpdater, "wqeStates", ['Available'])

        # provide access to WMBS in local WMAgent
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        # DB function to retrieve active workflows
        self.listActiveWflows = self.daoFactory(classname="Workflow.GetUnfinishedWorkflows")

        # local WorkQueue service
        self.localCouchUrl = config.WorkQueueManager.couchurl
        self.localWQ = WorkQueue(self.localCouchUrl,
                                 config.WorkQueueManager.dbname)

    def getActiveWorkflows(self):
        """
        Provide list of active requests within WMAgent
        :return: list of workflow names
        """
        self.logger.info("Fetching active workflows in the agent")
        # get list of active workflows in WMAgent
        wflowSpecs = self.listActiveWflows.execute()

        return [wflowSpec['name'] for wflowSpec in wflowSpecs]

    def findUnacquiredWorkflows(self):
        """
        Using a non-customized CouchDB view, find out workflows with elements
        in the statuses that we would like to update (e.g. Available).
        This is required because not all workflows might have been acquired by WMBS.
        :return: a flat list of workflow names
        """
        self.logger.info("Finding not yet active workflows in local workqueue")
        response = []
        # get list of active workflows in WMAgent by looking into local elements in Available status
        summary = self.localWQ.getElementsCountAndJobsByWorkflow()
        for wflowName, innerDict in summary.items():
            for status in innerDict:
                if status in self.wqeStates:
                    response.append(wflowName)
        return response

    def wmstatsDict(self, requests):
        """
        Return list of requests specs from WMStats for provided list of request names
        :param requests: list of workflow requests names
        :return: dict of workflow records obtained from wmstats server:
        {"wflow": {"SiteWhitelist":[], "SiteBlacklist": []}, ...}
        """
        # get list of workflows from wmstats
        outputMask = ['SiteWhitelist', 'SiteBlacklist']
        wdict = {}
        for state in self.reqStates:
            inputConditions = {"RequestStatus": state}
            self.logger.info("Fetch site info from WMStats for condition: %s and mask %s", inputConditions, outputMask)
            data = self.wmstatsSrv.getFilteredActiveData(inputConditions, outputMask)
            self.logger.info("Found %d workflows in WMStats with status %s", len(data), state)
            self.logger.debug("Data from wmstats for status %s: %s", state, pformat(data))
            for rdict in data:
                # rdict here has the following structure:
                # {"RequestName": "bla", "SiteWhitelist":[], "SiteBlacklist": []}
                wflow = rdict.pop('RequestName')
                # check that our workflow is in our requests list
                if wflow in requests:
                    wdict[wflow] = rdict
        return wdict

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Perform the following logic:
        - obtain list of current active workflows from the agent
        - obtain list of workflows not yet active but already in local workqueue
        - requests their specs from upstream wmstats server
        - update site lists of active workflows
        - push new specs to the agent local WorkQueue and update pickle spec file

        :return: none
        """
        # get list of active workflows from the agent, the returned dict
        # is composed by workflow names and associated pickle file (data comes from WMBS)
        wflows = self.getActiveWorkflows()
        self.logger.info("This agent has %d active workflows", len(wflows))
        self.logger.debug("Active workflows in the agent are: %s", wflows)

        # find workflows not yet active but already in local workqueue
        # note that some workflows here might already be active as well (multi WQEs)
        unacquiredWflows = self.findUnacquiredWorkflows()
        self.logger.info("This agent has %d not yet active workflows in local workqueue", len(unacquiredWflows))

        # now join all active and potentially not-yet-active workflows
        wflows = list(set(wflows + unacquiredWflows))

        # obtain workflow records from wmstats server
        wdict = self.wmstatsDict(wflows)
        self.logger.info("There is a total of %d common active workflows in the agent and wmstats", len(wdict))

        # iterate over the list of active workflows which is smaller than list from wmstats
        for wflow in wflows:
            if wflow not in wdict.keys():
                continue
            siteWhiteList = wdict[wflow]['SiteWhitelist']
            siteBlackList = wdict[wflow]['SiteBlacklist']

            # get the local Workqueue url for the workflow's spec
            specUrl = self.localWQ.hostWithAuth + "/%s/%s/spec" % (self.localWQ.db.name, wflow)

            # create wrapper helper and load the spec from local couch
            wHelper = WMWorkloadHelper()
            wHelper.load(specUrl)

            # extract from pickle spec both white and black site lists and compare them
            # to one we received from upstream service (ReqMgr2)
            wmaWhiteList = wHelper.getSiteWhitelist()
            wmaBlackList = wHelper.getSiteBlacklist()
            if set(wmaWhiteList) != set(siteWhiteList) or set(wmaBlackList) != set(siteBlackList):
                self.logger.info("Updating %s:", wflow)
                self.logger.info("  siteWhitelist %s => %s", wmaWhiteList, siteWhiteList)
                self.logger.info("  siteBlacklist %s => %s", wmaBlackList, siteBlackList)
                try:
                    # update local WorkQueue first
                    params = {'SiteWhitelist': siteWhiteList, 'SiteBlacklist': siteBlackList}
                    self.localWQ.updateElementsByWorkflow(wHelper, params, status=self.wqeStates)
                    msg = f"Successfully updated elements for workflow '{wflow}', "
                    msg += f"under WQ states: {self.wqeStates} and spec at: {sanitizeURL(specUrl).get('url')}"
                    self.logger.info(msg)
                except Exception as ex:
                    logging.exception("Unexpected exception while updating elements in local workqueue Details:\n%s", str(ex))
                    continue
            else:
                self.logger.info("No site list changes found for workflow %s", wflow)
