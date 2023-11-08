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
import argparse
import time
import logging
import os
import psutil

from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


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
        for dsetDict in info.get("ChainParentageMap", {}).values():
            childrenDsets.extend(dsetDict['ChildDsets'])
        wflowChildrenDsets = {"workflowName": reqName, "childrenDsets": childrenDsets}
    return wflowChildrenDsets


def updateParentageFlag(reqName, reqmgrDB, logger):
    """
    Given a workflow name, update its ParentageResolved flag in
    ReqMgr2.
    :param reqName: string with the workflow name
    :param reqmgrDB: object instance of the RequestDBWriter class
    :param logger: a logger object instance
    :return: none
    """
    try:
        reqmgrDB.updateRequestProperty(reqName, {"ParentageResolved": True})
        logger.info("  Marked ParentageResolved=True for request: %s", reqName)
    except Exception as exc:
        logger.error("  Failed to update 'ParentageResolved' flag to True for request: %s", reqName)


def parseArgs():
    """
    Parse the command line arguments, or provide default values.
    """
    msg = "Script to resolve a workflow parentage information and insert it into the DBS server"
    parser = argparse.ArgumentParser(description=msg)
    parser.add_argument("-w", "--workflow", help="String with the workflow name",
                        action="store", required=True)
    parser.add_argument("-r", "--reqmgr_url", help="ReqMgr2 URL (defaults to the production instance)",
                        action="store", default='https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache')
    parser.add_argument("-d", "--dbs_url", help="DBS Writer URL (defaults to the global production instance)",
                        action="store", default='https://cmsweb.cern.ch/dbs/prod/global/DBSWriter')
    args = parser.parse_args()
    return args


def main():
    """
    Executes everything
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.basicConfig()
    # parse the input arguments
    args = parseArgs()
    logger.info(f"Contacting ReqMgr2 URL: {args.reqmgr_url}")
    logger.info(f"Data will be written to DBS Server instance: {args.dbs_url}")

    reqmgrDB = RequestDBWriter(args.reqmgr_url)
    dbsSvc = DBS3Reader(args.dbs_url, logger=logger)

    # memory usage before starting the actual processing
    thisPID = os.getpid()
    thisProc = psutil.Process(thisPID)
    with thisProc.oneshot():
        logger.info(f"Initial memory info: {thisProc.memory_full_info()}")
        logger.info(f"Initial memory info: {thisProc.cpu_times()}")

    # retrieve request from ReqMgr2
    wflowData = getChildDatasetsForStepChainMissingParent(reqmgrDB, args.workflow)

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
