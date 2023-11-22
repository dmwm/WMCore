#!/usr/bin/env python
"""
The WorkflowUpdater poller component.
Among the actions performed by this component, we can list:
* find active workflows in the agent
* filter those that require pileup dataset
* find out the current location for the pileup datasets
* get a list of blocks available and locked by WM
* match those blocks with the current pileup config json file. In other words,
  blocks that are no longer locked and/or available need to be removed from the
  json file.
* update this json in the workflow sandbox
"""

import logging
import threading

from Utils.CertTools import cert, ckey
from Utils.IteratorTools import flattenList
from Utils.Timers import timeFunction, CodeTimer
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.WMException import WMException
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory


class WorkflowUpdaterException(WMException):
    """
    Specific WorkflowUpdaterPoller exception handling.
    """


class WorkflowUpdaterPoller(BaseWorkerThread):
    """
    Poller that does the actual work for updating workflows.
    """

    def __init__(self, config):
        """
        Initialize WorkflowUpdaterPoller object
        :param config: a Configuration object with the component configuration
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listActiveWflows = self.daoFactory(classname="Workflow.GetUnfinishedWorkflows")

        # parse mandatory attributes from the configuration
        self.config = config
        self.rucioAcct = getattr(config.WorkflowUpdater, "rucioAccount")
        self.rucioUrl = getattr(config.WorkflowUpdater, "rucioUrl")
        self.rucioAuthUrl = getattr(config.WorkflowUpdater, "rucioAuthUrl")
        self.rucioCustomScope = getattr(config.WorkflowUpdater, "rucioCustomScope",
                                        "group.wmcore")
        self.msPileupUrl = getattr(config.WorkflowUpdater, "msPileupUrl")

        self.userCert = cert()
        self.userKey = ckey()
        self.rucio = Rucio(acct=self.rucioAcct,
                           hostUrl=self.rucioUrl,
                           authUrl=self.rucioAuthUrl)
                           # configDict={'logger': self.logger})

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Executed in every polling cycle. The actual logic of the component is:
          1. find active workflows in the agent
          2. check if those active workflows are using pileup data
        :param parameters: not really used. But keeping same signature as
            the one defined in the super class.
        :return: only what is returned by the decorator
        """
        logging.info("Running Workflow updater injector poller algorithm...")
        try:
            # retrieve list of workflows with unfinished Production
            # or Processing subscriptions
            wflowSpecs = self.listActiveWflows.execute()
            if not wflowSpecs:
                logging.info("Agent has no active workflows at the moment")
                return

            # figure out workflows that have pileup
            puWflows = self.findWflowsWithPileup(wflowSpecs)
            if not puWflows:
                logging.info("Agent has no active workflows with pileup at the moment")
                return
            # resolve unique active pileup dataset names
            uniqueActivePU = flattenList([item['pileup'] for item in puWflows])

            # otherwise, move on retrieving pileups
            msPileupList = self.getPileupDocs()

            # and resolve blocks in each container being used by workflows
            # considerations for 2024 are around 100 pileups each taking 2 seconds in Rucio
            with CodeTimer("Rucio block resolution", logger=logging):
                self.findRucioBlocks(uniqueActivePU, msPileupList)

            # TODO in future tickets: find the json files in the spec/sandbox area and tweak them

        except Exception as ex:
            msg = f"Caught unexpected exception in WorkflowUpdater. Details:\n{str(ex)}"
            logging.exception(msg)
            raise WorkflowUpdaterException(msg) from None

    def getPileupDocs(self):
        """
        Fetch all pileup documents from MSPileup and preprocess the data.

        Note that the 'blocks' field is for the moment just a placeholder,
        as it will be populated in a later stage,

        :return: a list of dictionaries in the following format:
          {"pileupName": string with pileup name,
           "customName": string with custom pileup name - if any,
           "rses": list of RSE names,
           "blocks": list of block names}
        """
        mgr = RequestHandler()
        headers = {'Content-Type': 'application/json'}
        data = mgr.getdata(self.msPileupUrl, params={}, headers=headers, verb='GET',
                           ckey=self.userKey, cert=self.userCert, encode=True, decode=True)
        if data and data.get("result", []):
            if "error" in data["result"][0]:
                msg = f"Failed to retrieve MSPileup documents. Error: {data}"
                raise WorkflowUpdaterException(msg)

        logging.info("A total of %d pileup documents have been retrieved.", len(data["result"]))
        pileupMapList = []
        for puItem in data["result"]:
            logging.info("Pileup: %s, custom name: %s, expected at: %s, but currently available at: %s",
                         puItem['pileupName'], puItem['customName'],
                         puItem['expectedRSEs'], puItem['currentRSEs'])
            thisPU = {"pileupName": puItem['pileupName'],
                      "customName": puItem['customName'],
                      "rses": puItem['currentRSEs'],
                      "blocks": []}
            pileupMapList.append(thisPU)
        return pileupMapList

    def findWflowsWithPileup(self, listSpecs):
        """
        Given a list of workflow names and their respective specs, load each
        one of them and filter out those that don't require any pileup dataset.
        :param listSpecs: a list of dictionary with workflow name and spec path
        :return: a list of dictionaries with workflow name, spec path and list
            of pileup datasets being used, e.g.:
            {"name": string with workflow name,
             "spec": string with spec path,
             "pileup": list of strings with pileup names}
        """
        wflowsWithPU = []
        for wflowSpec in listSpecs:
            try:
                workloadHelper = WMWorkloadHelper()
                workloadHelper.load(wflowSpec['spec'])
                pileupSpecs = workloadHelper.listPileupDatasets()
                if pileupSpecs:
                    wflowSpec['pileup'] = pileupSpecs.values()
                    logging.info("Workflow: %s requires pileup dataset(s): %s",
                                 wflowSpec['name'], wflowSpec['pileup'])
                    wflowsWithPU.append(wflowSpec)
                else:
                    logging.info("Workflow: %s does not require any pileup", wflowSpec['name'])
            except Exception as ex:
                msg = f"Failed to load spec file for: {wflowSpec['spec']}. Details: {str(ex)}"
                logging.error(msg)
        logging.info("There are %d pileup workflows out of %d active workflows.",
                     len(wflowsWithPU), len(listSpecs))
        return wflowsWithPU

    def findRucioBlocks(self, uniquePUList, msPileupList):
        """
        Given a list of unique pileup dataset names, list all of
        their blocks in Rucio. Note that if a pileup document contains
        a customName dataset, then we need to resolve the blocks for that
        instead.
        :param uniquePUList: a list with pileup names
        :param msPileupList: a list with dictionaries from MSPileup
        :return: update the msPileupList object in place, by populating
            the 'block' field with a list of block names
        """
        for pileupItem in msPileupList:
            if pileupItem["pileupName"] not in uniquePUList:
                # no active workflow requires this pileup
                continue

            if pileupItem["customName"]:
                logging.info("Fetching blocks for custom pileup container: %s", pileupItem["customName"])
                pileupItem["blocks"] = self.rucio.getBlocksInContainer(pileupItem["customName"],
                                                                       scope=self.rucioCustomScope)
            else:
                logging.info("Fetching blocks for pileup container: %s", pileupItem["pileupName"])
                pileupItem["blocks"] = self.rucio.getBlocksInContainer(pileupItem["pileupName"], scope='cms')
