#!/usr/bin/env python
"""
File       : SiteListPoller
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: module to update of site lists within a WMAgent
"""

# system modules
import logging
import threading

# WMCore modules
from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
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
        self.states = getattr(config.WorkflowUpdater, "states", ['running-open', 'acquired'])

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
        :return: dict of workflows names vs pickle files
        """
        # get list of active workflows in WMAgent
        wflowSpecs = self.listActiveWflows.execute()

        # construct dictionary of workflow names and their pickle files
        wmaSpecs = {}
        for wflowSpec in wflowSpecs:
            name = wflowSpec['name']  # this is the name of workflow
            pklFileName = wflowSpec['spec']  # the "spec" in WMBS table (wmbs_workflow.spec) is pkl file name
            wmaSpecs[name] = pklFileName
        return wmaSpecs

    def wmstatsDict(self, requests):
        """
        Return list of requests specs from WMStats for provided list of request names
        :param requests: list of workflow requests names
        :return: dict of workflow records obtained from wmstats server:
        {"wflow": {"SiteWhitelist":[], "SiteBlacklist": []}, ...}
        """
        # get list of workflows from wmstats
        outputMask = ['SiteWhiteList', 'SiteBlackList']
        wdict = {}
        for state in self.states:
            inputConditions = {"RequestStatus": state}
            self.logger.info("Fetch site info from WMStats for condition: %s and mask %s", inputConditions, outputMask)
            data = self.wmstatsSrv.getFilteredActiveData(inputConditions, outputMask)
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
        - requests their specs from upstream wmstats server
        - update site lists of active workflows
        - push new specs to the agent local WorkQueue and update pickle spec file

        :return: none
        """
        # get list of active workflows from the agent, the returned dict
        # is composed by workflow names and associated pickle file (data comes from WMBS)
        wmaSpecs = self.getActiveWorkflows()
        wflows = wmaSpecs.keys()

        # obtain workflow records from wmstats server
        wdict = self.wmstatsDict(wflows)

        # iterate over the list of active workflows which is smaller than list from wmstats
        for wflow in wflows:
            if wflow not in wdict.keys():
                continue
            siteWhiteList = wdict[wflow]['SiteWhitelist']
            siteBlackList = wdict[wflow]['SiteBlacklist']

            # get the name of pkl file from wma spec
            pklFileName = wmaSpecs[wflow]

            # create wrapper helper and load pickle file
            wHelper = WMWorkloadHelper()
            wHelper.load(pklFileName)

            # extract from pickle spec both white and black site lists and compare them
            # to one we received from upstream service (ReqMgr2)
            wmaWhiteList = wHelper.getSiteWhitelist()
            wmaBlackList = wHelper.getSiteBlacklist()
            if set(wmaWhiteList) != set(siteWhiteList) or set(wmaBlackList) != set(siteBlackList):
                self.logger.info("Updating %s:", wflow)
                self.logger.info("  siteWhiteList %s => %s", wmaWhiteList, siteWhiteList)
                self.logger.info("  siteBlackList %s => %s", wmaBlackList, siteBlackList)
                try:
                    # update local WorkQueue first
                    params = {'SiteWhitelist': siteWhiteList, 'SiteBlacklist': siteBlackList}
                    self.localWQ.updateElementsByWorkflow(wHelper, params, status=['Available'])
                    self.logger.info("successfully updated workqueue elements for workflow %s", wflow)
                except Exception as ex:
                    logging.exception("Unexpected exception while updating elements in local workqueue Details:\n%s", str(ex))
                    continue

                # update workload only if we updated local WorkQueue
                # update site white/black lists together
                if set(wmaWhiteList) != set(siteWhiteList):
                    self.logger.info("updating site white list for workflow %s", wflow)
                    wHelper.setWhitelist(siteWhiteList)
                if set(wmaBlackList) != set(siteBlackList):
                    self.logger.info("updating site black list for workflow %s", wflow)
                    wHelper.setBlacklist(siteBlackList)

                try:
                    # persist the spec in local CouchDB
                    self.logger.info("Updating %s with new site lists within pkl file %s", wflow, pklFileName)
                    # save back pickle file
                    wHelper.save(pklFileName)
                except Exception as ex:
                    logging.exception("Caught unexpected exception in SiteListPoller. Details:\n%s", str(ex))
                    continue
