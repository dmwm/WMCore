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


class SiteListUpdater(Harness):
    """
    Create a SiteListUpdaterPoller component as a daemon
    """

    def __init__(self, config):
        """
        Initialize it with the agent configuration parameters.
        :param config: a Configuration object with the component configuration
        """
        # call the base class
        Harness.__init__(self, config)

    def preInitialization(self):
        """
        Step that actually adds the worker thread properly
        """
        pollInterval = self.config.SiteListUpdater.pollInterval
        logging.info("Starting %s with configuration:\n%s", self.__class__.__name__,
                     pformat(self.config.SiteListUpdater.dictionary_()))

        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(SiteListUpdaterPoller(self.config),
                                               pollInterval)


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
        self.reqmgrUrl = getattr(config.SiteListUpdater, "reqmgr2Url")

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
        Provide list of active requests in a system (from assigned to running)
        obtained the agent
        :return: list of workflows names and dict
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
        :return specs: dictionary of specs obtained from upstream ReqMgr2 server
        """
        urls = []
        for name in requests:
            url = "{}/data/request?name={}".format(self.reqmgrUrl, name)
            urls.append(url)
        response = getdata(urls, ckey(), cert())
        specs = {}
        for resp in response:
            data = json.loads(resp['data'])
            for rdict in data['result']:
                specs.update(rdict)
        return specs

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Executed in every polling cycle. The actual logic of the component is:
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

        # obtain workflow specs from upstream ReqMgr2 server, the returned dict
        # is composed by workflow names and JSON dict representing the workflow
        specs = self.getRequestSpecs(wflows)

        # iterate over workflow items and update local WorkQueue and pickle files if
        # either site white or black lists are different
        for wflow, rdict in specs.items():
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
            if wmaWhiteList != siteWhiteList or wmaBlackList != siteBlackList:
                self.logger.info(f"Updating {wflow}: siteWhiteList {wmaWhiteList} => {siteWhiteList} and siteBlackList {wmaBlackList} => {siteBlackList}")
                try:
                    # update local WorkQueue first
                    self.localWQ.updateSiteLists(wflow, siteWhiteList, siteBlackList)
                except Exception as ex:
                    msg = f"Caught unexpected exception in SiteListUpdater. Details:\n{str(ex)}"
                    logging.exception(msg)
                    raise LocalWorkqueueUpdateException(msg) from None

                # update workload only if we updated local WorkQueue
                # update site white/black lists together
                wHelper.setWhitelist(siteWhiteList)
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
                    raise SpecFileUpdateException(msg) from None


class LocalWorkqueueUpdateException(WMException):
    """
    Specific SiteListUpdater exception handling for updating local workqueue.
    """


class SpecFileUpdateException(WMException):
    """
    Specific SiteListUpdater exception handling for spec file update.
    """

