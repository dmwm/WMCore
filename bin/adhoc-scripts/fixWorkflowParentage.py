#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script runs the same logic as the one executed by the StepChainParentage
CherryPy thread, available under:
https://github.com/dmwm/WMCore/blob/master/src/python/WMCore/ReqMgr/CherryPyThreads/StepChainParentageFixTask.py

However, it takes a workflow name as input and performs the parentage fix only
against that one workflow.
Use case for this is on workflows that require a very large memory footprint.
"""
import time
import logging
import os
import sys
import psutil

from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


REQMGR_URL = 'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache'
DBS_URL = 'https://cmsweb.cern.ch/dbs/prod/global/DBSWriter'


def getChildDatasetsForStepChainMissingParent(reqmgrDB, wflowName):
    """
    Similar to the method implemented in the CherryPy thread.
    :param reqmgrDB: object to the RequestDBWriter class
    :param wflowName: string with the workflow name
    :return: a dictionaries with request name and children datasets
    """
    data = reqmgrDB.getRequestByNames(wflowName)
    for reqName, info in data.items():
        childrenDsets = []
        for _stepNum, dsetDict in info.get("ChainParentageMap", {}).items():
            childrenDsets.extend(dsetDict['ChildDsets'])
        wflowChildrenDsets = {"workflowName": reqName, "childrenDsets": childrenDsets}
    return wflowChildrenDsets


def updateParentageFlag(reqName, reqmgrDB, logger):
    """
    Given a workflow name, update its ParentageResolved flag in
    ReqMgr2.
    :param reqName: string with the workflow name
    :return: none
    """
    try:
        reqmgrDB.updateRequestProperty(reqName, {"ParentageResolved": True})
        logger.info("  Marked ParentageResolved=True for request: %s", reqName)
    except Exception as exc:
        logger.error("  Failed to update 'ParentageResolved' flag to True for request: %s", reqName)


def main():
    """Executes everything"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig()

    if len(sys.argv) != 2:
        logger.error("Please provide the workflow name as argument.")
        logger.error("E.g.: python fixWorkflowParentage.py my_workflow_name")
        sys.exit(1)

    wflowName = sys.argv[1]
    reqmgrDB = RequestDBWriter(REQMGR_URL)
    dbsSvc = DBS3Reader(DBS_URL, logger=logger)

    # memory usage before starting the actual processing
    thisPID = os.getpid()
    thisProc = psutil.Process(thisPID)
    with thisProc.oneshot():
        logger.info(f"Initial memory info: {thisProc.memory_full_info()}")
        logger.info(f"Initial memory info: {thisProc.cpu_times()}")

    # retrieve request from ReqMgr2
    wflowData = getChildDatasetsForStepChainMissingParent(reqmgrDB, wflowName)

    # measure time taken to fix the actual workflow parentage
    start = time.time()
    parentageResolved = True
    for childDS in wflowData["childrenDsets"]:
        logger.info("  Handling parentage for child dataset: %s", childDS)
        try:
            failedBlocks = dbsSvc.fixMissingParentageDatasets(childDS, insertFlag=True)
        except Exception as exc:
            logger.exception("  Failed to resolve parentage data for dataset: %s. Error: %s", childDS, str(exc))
            parentageResolved = False
        else:
            if failedBlocks:
                logger.warning("  These blocks failed to be resolved and will be retried later: %s", failedBlocks)
                parentageResolved = False
            else:
                logger.info("  Parentage for '%s' successfully updated.", childDS)
    timeTaken = time.time() - start
    logger.info(f"  time spent with workflow {wflowData['workflowName']} is: {timeTaken} secs")
    if parentageResolved:
        # then we can update the request flag
        updateParentageFlag(wflowData['workflowName'], reqmgrDB=reqmgrDB, logger=logger)
    else:
        logger.warning(f" Workflow {wflowData['workflowName']} will be retried in the next cycle")

    with thisProc.oneshot():
        memUsed = thisProc.memory_full_info()
        logger.info(f"Final memory info: {memUsed} with PSS: {memUsed.pss / (1024 **2)} MB")
        logger.info(f"Final memory info: {thisProc.cpu_times()}")


if __name__ == '__main__':
    main()
