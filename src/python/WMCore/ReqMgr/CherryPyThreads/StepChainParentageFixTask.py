from __future__ import (division, print_function)
import time
from collections import defaultdict

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.DBS.DBS3Reader import DBS3Reader


def getChildDatasetsForStepChainMissingParent(reqmgrDB, status):
    """
    :param status: workflow status
    :return: set of child dataset
    """
    results = reqmgrDB.getStepChainDatasetParentageByStatus(status)

    requestsByChildDataset = defaultdict(set)

    for reqName, info in results.items():

        for dsInfo in info.values():
            if dsInfo["ParentDset"]:
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
        self.statusToCheck = ["announced", "normal-archived"]

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
        self.logger.info("Updating parentage for StepChain workflows for %s", self.statusToCheck)
        childDatasets = set()
        requests = set()
        requestsByChildDataset = {}
        for status in self.statusToCheck:
            reqByChildDS= getChildDatasetsForStepChainMissingParent(self.reqmgrDB, status)
            childDatasets = childDatasets.union(set(reqByChildDS.keys()))
            # We need to just get one of the StepChain workflow if multiple workflow contains the same datasets. (i.e. ACDC)
            requestsByChildDataset.update(reqByChildDS)

            for wfs in reqByChildDS.values():
                requests = requests.union(wfs)

        failedRequests = set()
        totalChildDS = len(childDatasets)
        fixCount = 0
        for childDS in childDatasets:
            start = int(time.time())
            failedBlocks = self.dbsSvc.fixMissingParentageDatasets(childDS, insertFlag=True)
            end = int(time.time())
            timeTaken = end - start
            if failedBlocks:
                self.logger.warning("Failed to fix the parentage for %s will be retried: time took: %s (sec)",
                                 failedBlocks, timeTaken)
                failedRequests = failedRequests.union(requestsByChildDataset[childDS])
            else:
                fixCount += 1
                self.logger.info("Fixed %s parentage: %s out of %s datasets. time took: %s (sec)",
                                 childDS, fixCount, totalChildDS, timeTaken)

        requestsToUpdate = requests - failedRequests

        for request in requestsToUpdate:
            self.reqmgrDB.updateRequestProperty(request, {"ParentageResolved": True})

        self.logger.info("Total %s requests' ParentageResolved flag is set to True", len(requestsToUpdate))
        self.logger.info("Total %s requests will be retried next cycle: %s", len(failedRequests), failedRequests)