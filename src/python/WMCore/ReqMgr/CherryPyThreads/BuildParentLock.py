"""
This CherryPy thread is meant to parse all the active requests;
find which requests require parent dataset to be processed;
and run the DBS parentage lookup.
"""
from __future__ import division, print_function

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.DBS.DBSReader import DBS3Reader
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux


class BuildParentLock(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(BuildParentLock, self).__init__(config)
        self.reqmgrAux = ReqMgrAux(config.reqmgr2_url, logger=self.logger)
        self.dbs = DBS3Reader(config.dbs_url)
        # cache of dbs lookups mapping input dataset to parent dataset
        self.dbsLookupCache = {}
        # set of of currently active datasets requiring parent dataset
        self.inputDatasetCache = set()
        self.reqDB = RequestDBReader(config.reqmgrdb_url)
        self.filterKeys = ['assignment-approved', 'assigned', 'staging', 'staged',
                           'failed', 'acquired', 'running-open', 'running-closed',
                           'force-complete', 'completed', 'closed-out']


    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.fetchIncludeParentsRequests,
                                 'duration': config.updateParentsInterval}]

    def fetchIncludeParentsRequests(self, config):
        """
        Fetch active requests from the "requestsincludeparents" couch view that
        have IncludeParents=True, find parents of each dataset and send to
        reqmgr2 auxiliary database.
        """
        # use this boolean to signal whether there were datasets that failed
        # to get their parentage resolved
        incompleteParentage = False
        # use this boolean to signal if new parent datasets need to be locked
        auxDbUpdateRequired = False

        setDsets = set()
        setParents = set()
        dictParents = {}

        self.logger.info("Executing parent lock cherrypy thread")

        # query couch view to find datasets for workflows requiring parent datasets
        # only returning requests with the statuses in filterKeys
        try:
            results = self.reqDB._getCouchView("requestsincludeparents", {}, self.filterKeys)
        except Exception as ex:
            self.logger.error("Error retrieving requests including parent datasets from couchdb.")
            self.logger.error("Error: %s", str(ex))
            return

        for row in results["rows"]:
            dataset = row["value"]
            setDsets.add(dataset)

        # check to see if any changes have been made
        if setDsets != self.inputDatasetCache:
            auxDbUpdateRequired = True
            self.inputDatasetCache = setDsets.copy()

        self.logger.info("Found %d unique datasets requiring the parent dataset", len(setDsets))
        if auxDbUpdateRequired:
            self.logger.info("Found new parent dataset locks to update.")
            # look up parent datasets first via the local DBS cache, if not found do lookup via DBS
            for dset in setDsets:
                if dset in self.dbsLookupCache:
                    setParents.add(self.dbsLookupCache[dset])
                    self.logger.info("Resolved parentage via lookup cache for: %s", dset)
                else:
                    try:
                        res = self.dbs.listDatasetParents(dset)
                    except Exception as exc:
                        self.logger.warning("Failed to resolve parentage for: %s. Error: %s", dset, str(exc))
                        incompleteParentage = True
                        continue
                    self.logger.info("Resolved parentage via DBS for: %s", res)
                    if res:
                        setParents.add(res[0]['parent_dataset'])
                        self.dbsLookupCache[dset] = res[0]['parent_dataset']

            if not incompleteParentage:
                dictParents['parentlocks'] = list(setParents)
                if self.reqmgrAux.updateParentLocks(dictParents):
                    self.logger.info("Parentage lookup complete and auxiliary database updated.")
                else:
                    self.logger.info("Error updating parentage document. Using stale data until next cycle.")
            else:
                # then don't replace any data for the moment, simply add new parents
                previousData = self.reqmgrAux.getParentLocks()
                # check to see if response from aux db has been populated
                if previousData and 'parentlocks' in previousData[0]:
                    setPreviousData = set(previousData[0]['parentlocks'])
                    setParents = setParents | setPreviousData
                    dictParents['parentlocks'] = list(setParents)
                    self.reqmgrAux.updateParentLocks(dictParents)
                    self.logger.info("Parentage lookup complete (with errors) and auxiliary database updated.")
                else:
                    self.logger.info("Parent locks not returned from auxiliary database. Skipping parentage update.")

        else:
            self.logger.info("No new parent datasets need locked. Skipping update of auxiliary database.")

        return
