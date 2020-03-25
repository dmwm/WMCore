"""
Created on Aug 13, 2014

@author: sryu
"""
from __future__ import (division, print_function)

from WMCore.ACDC.CouchService import CouchService
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader


class CouchDBCleanup(CherryPyPeriodicTask):
    def __init__(self, rest, config):

        super(CouchDBCleanup, self).__init__(config)
        self.reqDB = RequestDBReader(config.reqmgrdb_url)
        self.reqmgrAux = ReqMgrAux(config.reqmgr2_url, logger=self.logger)
        # statuses that we want to keep the transfer documents
        self.transferStatuses = ["assigned", "staging", "staged", "acquired",
                                 "failed", "running-open", "running-closed"]

        baseURL, acdcDB = splitCouchServiceURL(config.acdc_url)
        self.acdcService = CouchService(url=baseURL, database=acdcDB)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.acdcCleanup, 'duration': config.acdcCleanDuration},
                                {'func': self.auxCouchCleanup, 'duration': config.auxCleanDuration}]

    def auxCouchCleanup(self, config):
        """
        Cleanup TRANSFER documents from the reqmgr_auxiliary CouchDB.
        The list of status can be expanded in the future
        """
        self.logger.info("Fetching TRANSFER documents from CouchDB...")

        transferDocs = self.reqmgrAux.getTransferInfo("ALL_DOCS")
        if not transferDocs:
            self.logger.info("  there are no transfer documents in the database.")
            return
        auxDocs = []
        for row in transferDocs:
            auxDocs.append(row['workflowName'])

        results = self.reqDB._getCouchView("bystatus", {}, self.transferStatuses)
        activeRequests = []
        for row in results["rows"]:
            activeRequests.append(row["id"])

        # now find transfer docs that are not active in the system
        transferDocs = []
        for transferDoc in auxDocs:
            if transferDoc not in activeRequests:
                transferDocs.append(transferDoc)
        self.logger.info("Found %d transfer documents to delete", len(transferDocs))

        for wflowName in transferDocs:
            self.logger.info("Deleting transfer document: %s", wflowName)
            try:
                self.reqmgrAux.deleteConfigDoc("transferinfo", wflowName)
            except Exception as exc:
                self.logger.warning("Failed to delete transfer doc: %s. Error: %s", wflowName, str(exc))
        self.logger.info("Transfer documents cleanup completed.")

    def acdcCleanup(self, config):
        """
        gather active data statistics
        """
        self.logger.info("Fetching ACDC collection names...")
        originalRequests = self.acdcService.listCollectionNames()
        if not originalRequests:
            self.logger.info("  there are no collection documents to delete.")
            return

        # filter requests
        results = self.reqDB._getCouchView("byrequest", {}, originalRequests)
        # filter requests only in the following status
        deleteStates = ["announced", "rejected-archived", "aborted-archived", "normal-archived"]
        filteredRequests = []
        for row in results["rows"]:
            if row["value"][0] in deleteStates:
                filteredRequests.append(row["key"])

        total = 0
        for req in filteredRequests:
            try:
                self.logger.info("Removing ACDC collection for: %s", req)
                deleted = self.acdcService.removeFilesetsByCollectionName(req)
                if deleted is None:
                    self.logger.warning("  request '%s' already deleted", req)
                else:
                    total += len(deleted)
                    self.logger.info("request %s deleted", req)
            except Exception as ex:
                self.logger.error("Failed to delete request: %s, will try again later. Error: %s", req, str(ex))
        self.logger.info("total %s requests deleted", total)
        return
