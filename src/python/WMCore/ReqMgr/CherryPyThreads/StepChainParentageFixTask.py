#!/usr/bin/env python
import time

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


def getChildDatasetsForStepChainMissingParent(reqmgrDB, status):
    """
    Fetches data from ReqMgr2/CouchDB for workflows that need to
    have their parentage information fixed.
    :param reqmgrDB: object instance of the RequestDBWriter class
    :param status: string with the workflow status
    :return: a flat list of dictionaries with request name and children datasets
    """
    results = reqmgrDB.getStepChainDatasetParentageByStatus(status)
    wflowChildrenDsets = []
    for reqName, info in results.items():
        childrenDsets = []
        for _stepNum, dsetDict in info.items():
            childrenDsets.extend(dsetDict['ChildDsets'])
        wflowChildrenDsets.append({"workflowName": reqName, "childrenDsets": childrenDsets})
    return wflowChildrenDsets


class StepChainParentageFixTask(CherryPyPeriodicTask):
    """
    Updates StepChain parentage periodically
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
        fixedCount = 0
        for status in self.statusToCheck:
            listWflowChildren = getChildDatasetsForStepChainMissingParent(self.reqmgrDB, status)
            msg = f"Retrieved {len(listWflowChildren)} workflows to fix parentage in status {status}"
            self.logger.info(msg)

            # now resolve every dataset parentage for each workflow
            while listWflowChildren:
                wflowData = listWflowChildren.pop()
                msg = f'Resolving parentage for workflow {wflowData["workflowName"]} '
                msg += f'with a total of {len(wflowData["childrenDsets"])} datasets. '
                msg += f'Still {len(listWflowChildren)} workflows left to be resolved.'
                self.logger.info(msg)
                # measure time taken to fix a workflow parentage
                start = time.time()
                parentageResolved = True
                for childDS in wflowData["childrenDsets"]:
                    self.logger.info("  Handling parentage for child dataset: %s", childDS)
                    try:
                        failedBlocks = self.dbsSvc.fixMissingParentageDatasets(childDS, insertFlag=True)
                    except Exception as exc:
                        self.logger.exception("  Failed to resolve parentage data for dataset: %s. Error: %s",
                                              childDS, str(exc))
                        parentageResolved = False
                    else:
                        if failedBlocks:
                            self.logger.warning("  These blocks failed to be resolved and will be retried later: %s",
                                                failedBlocks)
                            parentageResolved = False
                        else:
                            self.logger.info("  Parentage for '%s' successfully updated.", childDS)
                timeTaken = time.time() - start
                self.logger.info(f"  time spent with workflow {wflowData['workflowName']} is: {timeTaken} secs")
                if parentageResolved:
                    fixedCount += 1
                    # then we can update the request flag
                    self.updateParentageFlag(wflowData['workflowName'])
                else:
                    self.logger.warning(f" Workflow {wflowData['workflowName']} will be retried in the next cycle")

        self.logger.info(f"Resolved a total of {fixedCount} workflows in this cycle\n")

    def updateParentageFlag(self, reqName):
        """
        Given a workflow name, update its ParentageResolved flag in
        ReqMgr2.
        :param reqName: string with the workflow name
        :return: none
        """
        try:
            self.reqmgrDB.updateRequestProperty(reqName, {"ParentageResolved": True})
            self.logger.info("  Marked ParentageResolved=True for request: %s", reqName)
        except Exception as exc:
            self.logger.error("  Failed to update 'ParentageResolved' flag to True for request: %s", reqName)
