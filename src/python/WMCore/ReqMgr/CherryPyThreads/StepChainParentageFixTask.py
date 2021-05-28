#!/usr/bin/env python
from __future__ import (division, print_function)
from future.utils import viewitems, viewvalues

import time
from collections import defaultdict

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


def getChildDatasetsForStepChainMissingParent(reqmgrDB, status):
    """
    :param status: workflow status
    :return: set of child dataset
    """
    results = reqmgrDB.getStepChainDatasetParentageByStatus(status)

    requestsByChildDataset = defaultdict(set)

    for reqName, info in viewitems(results):

        for dsInfo in viewvalues(info):
            for childDS in dsInfo["ChildDsets"]:
                requestsByChildDataset[childDS].add(reqName)
    return requestsByChildDataset


class StepChainParentageFixTask(CherryPyPeriodicTask):
    """
    Upldates StepChain parentage periodically
    """

    def __init__(self, rest, config):

        super(StepChainParentageFixTask, self).__init__(config)
        self.reqmgrDB = RequestDBWriter(config.reqmgrdb_url)
        self.dbsSvc = DBS3Reader(config.dbs_url, logger=self.logger)
        self.statusToCheck = ["closed-out", "announced"]

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.fixStepChainParentage, 'duration': config.parentageFixDuration}]

    def fixStepChainParentage(self, config):
        """
        Look through the stepchain workflows with ParentageResolved flag is False.
        Fix the StepChain parentage and update the ParentageResolved flag to True
        """
        self.logger.info("Running fixStepChainParentage thread for statuses: %s", self.statusToCheck)
        childDatasets = set()
        requests = set()
        requestsByChildDataset = {}
        for status in self.statusToCheck:
            reqByChildDS = getChildDatasetsForStepChainMissingParent(self.reqmgrDB, status)
            self.logger.info("Retrieved %d datasets to fix parentage, in status: %s",
                             len(reqByChildDS), status)
            childDatasets = childDatasets.union(set(reqByChildDS.keys()))
            # We need to just get one of the StepChain workflow if multiple workflow contains the same datasets. (i.e. ACDC)
            requestsByChildDataset.update(reqByChildDS)

            for wfs in viewvalues(reqByChildDS):
                requests = requests.union(wfs)

        failedRequests = set()
        totalChildDS = len(childDatasets)
        fixCount = 0
        for childDS in childDatasets:
            self.logger.info("Resolving parentage for dataset: %s", childDS)
            start = time.time()
            try:
                failedBlocks = self.dbsSvc.fixMissingParentageDatasets(childDS, insertFlag=True)
            except Exception as exc:
                self.logger.exception("Failed to resolve parentage data for dataset: %s. Error: %s",
                                      childDS, str(exc))
                failedRequests = failedRequests.union(requestsByChildDataset[childDS])
            else:
                if failedBlocks:
                    self.logger.warning("These blocks failed to be resolved and will be retried later: %s",
                                        failedBlocks)
                    failedRequests = failedRequests.union(requestsByChildDataset[childDS])
                else:
                    fixCount += 1
                    self.logger.info("Parentage for '%s' successfully updated. Processed %s out of %s datasets.",
                                     childDS, fixCount, totalChildDS)
            timeTaken = time.time() - start
            self.logger.info("    spent %s secs on this dataset: %s", timeTaken, childDS)

        requestsToUpdate = requests - failedRequests

        for request in requestsToUpdate:
            try:
                self.reqmgrDB.updateRequestProperty(request, {"ParentageResolved": True})
                self.logger.info("Marked ParentageResolved=True for request: %s", request)
            except Exception as exc:
                self.logger.error("Failed to update 'ParentageResolved' flag to True for request: %s", request)

        msg = "A total of %d requests have been processed, where %d will have to be retried in the next cycle."
        self.logger.info(msg, len(requestsToUpdate), len(failedRequests))
