"""
This CherryPy thread is meant to parse all the active requests
from the WMStats DataCache; find which requests require parent
dataset to be processed; and run the DBS parentage lookup.
"""
from __future__ import division, print_function

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.Services.DBS.DBSReader import DBS3Reader

class BuildParentLock(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(BuildParentLock, self).__init__(config)
        self.dbs = DBS3Reader(config.dbs_url)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.fetchIncludeParentsRequests,
                                 'duration': config.updateParentsInterval}]

    def fetchIncludeParentsRequests(self, config):
        """
        Fetch requests from the DataCache that have IncludeParents=True
        """
        # use this boolean to signal whether there were datasets that failed
        # to get their parentage resolved
        incompleteParentage = False

        setDsets = set()
        setParents = set()
        self.logger.info("Executing parent lock cherrypy thread")
        filterDict = {"IncludeParents": True}
        for inputDset in DataCache.filterData(filterDict, ["InputDataset"]):
            setDsets.add(inputDset)

        self.logger.info("Found %d unique datasets requiring the parent dataset", len(setDsets))
        for dset in setDsets:
            try:
                res = self.dbs.listDatasetParents(dset)
            except Exception as exc:
                self.logger.warning("Failed to resolve parentage for: %s. Error: %s", dset, str(exc))
                incompleteParentage = True
                continue
            self.logger.info("Resolved parentage for: %s", res)
            if res:
                setParents.add(res[0]['parent_dataset'])

        if not incompleteParentage:
            DataCache.setParentDatasetList(list(setParents))
            self.logger.info("Parentage lookup complete and cache renewed")
        else:
            # then don't replace any data for the moment, simply add new parents
            previousData = set(DataCache.getParentDatasetList())
            setParents = setParents | previousData
            DataCache.setParentDatasetList(list(setParents))
            self.logger.info("Parentage lookup complete and cache updated")

        return
