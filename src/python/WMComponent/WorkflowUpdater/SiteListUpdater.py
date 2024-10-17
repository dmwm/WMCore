#!/usr/bin/env python
"""
File       : SiteListUpdater
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: module to update of site lists within a WMAgent
"""

# system modules
import os
import json
import shutil
import logging
import threading

# WMCore modules
from Utils.CertTools import ckey, cert
from Utils.Timers import timeFunction
from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.pycurl_manager import getdata
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.WMException import WMException
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class SiteListUpdaterPoller(BaseWorkerThread):
    def __init__(self, config):
        """
        Initialize SiteListUpdaterPoller object
        :param config: a Configuration object with the component configuration
        """
        BaseWorkerThread.__init__(self)
        myThread = threading.currentThread()
        self.logger = myThread.logger

        # the reqmgr2Url should be points to ReqMgr2 data services, i.e. /reqmgr2 end-point
        self.wmstatsUrl = getattr(config.SiteListUpdater, "wmstatsUrl")

        # provide access to WMBS in local WMAgent
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        # DB function to retrieve active workflows
        self.listActiveWflows = self.daoFactory(classname="Workflow.GetUnfinishedWorkflows")

        # local WorkQueue service
        self.localCouchUrl = self.config.WorkQueueManager.couchurl
        self.localWQ = WorkQueue(self.localCouchUrl,
                                 self.config.WorkQueueManager.dbname)

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

    def getRequestSpecs(self, requests):
        """
        Return list of requests specs for provided list of request names
        :param requests: list of workflow requests names
        :return wsList: list of workflow records obtained from wmstats server, each record has the following structure
        {"RequestName": "bla", "SiteWhitelist":[], "SiteBlacklist": []}
        """
        # get list of workflows from wmstats
        states = ['running-open', 'acquired']
        urls = []
        for state in states:
            url = "{}/data/filtered_requests?RequestStatus={}&mask=SiteWhitelist&mask=SiteBlacklist".format(self.wmstatsUrl, state)
            urls.append(url)
        response = getdata(urls, ckey(), cert())
        wsList = []
        for resp in response:
            data = json.loads(resp['data'])
            for rdict in data['result']:
                # rdict here has the following structure: list of records where each record is
                # {"RequestName": "bla", "SiteWhitelist":[], "SiteBlacklist": []}
                wflow = rdict['RequestName']
                # check that our workflow is in our requests list
                if wflow in requests:
                    wsList.append(rdict)
        return wsList

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Perform the following logic:
        - obtain list of current active workflows from the agent
        - requests their specs from upstream ReqMgr2 server
        - update site lists of all workflows
        - push new specs to the agent local WorkQueue and update pickle spec file

        :return: none
        """
        # get list of active workflows from the agent, the returned dict
        # is composed by workflow names and associated pickle file (data comes from WMBS)
        wmaSpecs = self.getActiveWorkflows()
        wflows = wmaSpecs.keys()

        # obtain workflow records from wmstats server
        wsList = self.getRequestData(wflows)

        # iterate over workflow items and update local WorkQueue and pickle files if
        # either site white or black lists are different
        for rdict in wsList:
            wflow = rdict['RequestName']
            siteWhiteList = rdict['SiteWhitelist']
            siteBlackList = rdict['SiteBlacklist']

            # get the name of pkl file from wma spec
            pklFileName = wmaSpecs[wflow]

            # create wrapper helper and load pickle file
            wHelper = WMWorkloadHelper()
            wHelper.load(pklFileName)

            # extract from pickle spec both white and black site lists and compare them
            # to one we received from upstream service (ReqMgr2)
            wmaWhiteList = wHelper.getSiteWhiteList()
            wmaBlackList = wHelper.getSiteBlackList()
            if set(wmaWhiteList) != set(siteWhiteList) or set(wmaBlackList) != set(siteBlackList):
                self.logger.info(f"Updating {wflow}: siteWhiteList {wmaWhiteList} => {siteWhiteList} and siteBlackList {wmaBlackList} => {siteBlackList}")
                try:
                    # update local WorkQueue first
                    self.localWQ.updateSiteLists(wflow, siteWhiteList, siteBlackList)
                except Exception as ex:
                    msg = f"Caught unexpected exception in SiteListUpdater. Details:\n{str(ex)}"
                    logging.exception(msg)
                    continue

                # update workload only if we updated local WorkQueue
                # update site white/black lists together
                if set(wmaWhiteList) != set(siteWhiteList):
                    wHelper.setWhitelist(siteWhiteList)
                if set(wmaBlackList) != set(siteBlackList):
                    wHelper.setBlacklist(siteBlackList)

                try:
                    # persist the spec in local CouchDB
                    self.logger.info(f"Updating {self.localCouchUrl} with new site lists for {wflow}")
                    wHelper.saveCouchUrl(self.localCouchUrl)

                    # save back pickle file
                    newPklFileName = pklFileName.split('.pkl')[0] + '_new.pkl'
                    wHelper.save(newPklFileName)

                    # if new pickle file is saved we can swap it with original one
                    if os.path.getsize(newPklFileName) > 0:
                        self.logger.info(f"Updated {pklFileName}")
                        shutil.move(newPklFileName, pklFileName)
                except Exception as ex:
                    msg = f"Caught unexpected exception in SiteListUpdater. Details:\n{str(ex)}"
                    logging.exception(msg)
                    continue
