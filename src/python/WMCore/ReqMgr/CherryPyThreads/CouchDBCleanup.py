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
        # create another object to always retrieve up-to-date views (no stale)
        self.reqDBNoStale = RequestDBReader(config.reqmgrdb_url)
        self.reqDBNoStale._setNoStale()
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
        self.logger.info("Fetching all the TRANSFER documents from CouchDB...")

        transferDocs = []
        for row in self.reqmgrAux.getTransferInfo("ALL_DOCS"):
            transferDocs.append(row['workflowName'])
        if not transferDocs:
            self.logger.info("  there are no transfer documents in the database.")
            return
        self.logger.info("%d transfer documents retrieved from the database.", len(transferDocs))

        self.logger.info("Fetching workflows from CouchDB for statuses: %s", self.transferStatuses)
        results = self.reqDBNoStale._getCouchView("bystatus", {}, self.transferStatuses)
        if not results["rows"]:
            self.logger.info("  there are no workflows matching those statuses.")
            return
        activeRequests = []
        for row in results["rows"]:
            activeRequests.append(row["id"])
        self.logger.info("%d workflows retrieved from the database.", len(activeRequests))

        # now find transfer documents (workflows) that are not active in the system
        docsToDelete = []
        for wflowName in transferDocs:
            if wflowName not in activeRequests:
                docsToDelete.append(wflowName)
        self.logger.info("Found %d transfer documents to delete", len(docsToDelete))

        for wflowName in docsToDelete:
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
                    self.logger.info("ACDC collection %s deleted", req)
            except Exception as ex:
                self.logger.error("Failed to delete request: %s, will try again later. Error: %s", req, str(ex))
        self.logger.info("Total %s ACDC collections deleted", total)
        return
