#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to fetch workflows from Reqmgr based on a set of workflow parameters
Usage:
      source /data/srv/current/apps/reqmgr2ms/etc/profile.d/init.sh
      python3 -i -- updateTotalStats.py -c $WMCORE_SERVICE_CONFIG/reqmgr2ms-output/config-output.py --dry False'
"""

import sys
import logging
import argparse
import json
import math

from pprint import pformat

from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.MicroService.MSCore.MSCore import MSCore
from Utils.Utilities import strToBool

def parseArgs():
    """
    Generic Argument Parser function
    """
    parser = argparse.ArgumentParser(
        prog='fetchFromReqmgr',
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__)

    parser.add_argument('-c', '--config', required=True,
                        help="""\
                        The path to MSCONFIG to be used, e.g.
                        for production: /data/srv/current/config/reqmgr2ms/config-output.py
                        """)

    parser.add_argument('-d', '--dry', action='store', default=True,
                        help="""\
                        Set dry run mode - No write operations will be performed with Reqmgr.
                        Possible values: [True|true|False|false]
                        Default: True
                        """)

    parser.add_argument('--debug', action='store_true', default=False,
                        help="""\
                        Set logging to debug mode.""")


    args = parser.parse_args()
    return args


def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    logger = logging.getLogger()
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    if logger.handlers:
        logger.handlers.clear()
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger


class MSDbg(MSCore):
    """
    A minimalistic debugger class based on the micorservices core functionalities.
    It allows intialisation of a generic microservice objec, which could be used also
    in interactive mode with any python shell.
    """
    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSDebugger module
        :param msConfig: A micro service configuration
        """
        super(MSDbg, self).__init__(msConfig, logger=logger)
        self.dbsReader = DBSReader(msConfig['dbsUrl'])

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status.
        :param reqStatus: The status for the requests to be fetched from ReqMgr2
        :return requests: A dictionary with all the workflows in the given status
        """
        self.logger.info("Fetching requests in status: %s", reqStatus)
        result = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)
        if not result:
            requests = {}
        else:
            requests = result[0]
        self.logger.info('  retrieved %s requests in status: %s', len(requests), reqStatus)
        return requests


def main():
    """
    An Utility to search all workflows in status completed with missing
    TotalInput* statistics and recalculate them.
    """

    reqStatus = "completed"

    msDbg = MSDbg(msConfig, logger=logger)

    badWfList = []
    reqList = list(msDbg.getRequestRecords(reqStatus).values())

    allReqTypes = {req['RequestType'] for req in reqList}
    logger.info("All possible request types: \n%s", pformat(allReqTypes))

    keySetToCheck = {'TotalInputEvents','TotalInputLumis','TotalInputFiles', 'TotalEstimatedJobs'}
    keySetHighLevel = {'RequestName', 'RequestType', 'SubRequestType'}
    keySetToFetch = {'InputDataset', 'RequestNumEvents','FilterEfficiency', 'EventsPerJob', 'EventsPerLumi', 'SplittingAlgo'}

    for wflow in reqList:
        # First  check if the current workflwo suffers the issue:
        if not keySetToCheck < list(wflow.keys()):
            # Create a `tmp` key to fill in all the information we'll need in the follwoing steps.
            wflow['tmp'] = {}

            # Then append None as the missing values in wflow['tmp']:
            for key in keySetToCheck:
                wflow['tmp'][key] = None

            # Then fetch all the data we'd need to calculate the missing values
            # depending on the type of workflow and add them as key/value pairs in wflow['tmp']:
            if wflow['RequestType'] == 'ReReco' or \
               wflow['RequestType'] == 'ReDigi' or \
               wflow['RequestType'] == 'DQMHarvest':
                for key in keySetToFetch:
                    try:
                        val = wflow[key]
                    except KeyError:
                        val = None
                    wflow['tmp'][key] = val
            elif wflow['RequestType'] == 'TaskChain':
                for key in keySetToFetch:
                    try:
                        val = wflow['Task1'][key]
                    except KeyError:
                        val = None
                    wflow['tmp'][key] = val
            elif wflow['RequestType'] == 'StepChain':
                for key in keySetToFetch:
                    try:
                        val = wflow['Step1'][key]
                    except KeyError:
                        val = None
                    wflow['tmp'][key] = val
            elif wflow['RequestType'] == 'Resubmission':
                pass
            else:
                logger.warning("Found unexpected workflow of type: %s", wflow['RequestType'])

            # Append the High level keys identifying the workflow as key/value pairs in wflow['tmp']:
            for key in keySetHighLevel:
                wflow['tmp'][key] = wflow[key]

            # Recalculate the missing values:
            if wflow['tmp']['SplittingAlgo'] == 'EventBased':
                logger.info("Found a workflow with EventBased Splitting algorithm:")

                # Avoid division by zero
                if not wflow['tmp']['FilterEfficiency']:
                    wflow['tmp']['FilterEfficiency'] = 1

                totalEvents = int(wflow['tmp']['RequestNumEvents'] / wflow['tmp']['FilterEfficiency'])
                lumisPerJob = math.ceil(wflow['tmp']['EventsPerJob']/wflow['tmp']['EventsPerLumi'])
                totalLumis = math.ceil(totalEvents / wflow['tmp']['EventsPerLumi'])
                totalEstimatedJobs = math.ceil(totalLumis/lumisPerJob)

                wflow['tmp']['TotalInputEvents'] = totalEvents
                wflow['tmp']['TotalInputLumis'] = totalLumis
                wflow['tmp']['TotalEstimatedJobs'] = totalEstimatedJobs
                wflow['tmp']['TotalInputFiles'] = 0
                logger.info('\n%s', pformat(wflow['tmp']))


            elif wflow['tmp']['SplittingAlgo'] == 'EventAwareLumiBased':
                logger.info("Found a workflow with EventAwareLumiBased Splitting algorithm:")
                keySetInputLists = {'LumiList', 'RunWhitelist', 'RunBlacklist', 'BlockWhitelist', 'BlockBlacklist'}
                for key in keySetInputLists:
                    try:
                        wflow['tmp'][key] = wflow[key]
                    except KeyError:
                        wflow['tmp'][key] = None

                if any([wflow['tmp'][key] for key in keySetInputLists]):
                    logger.info("Found input Lists for the current workflow. Skipping:")
                    logger.info('\n%s', pformat(wflow['tmp']))
                    continue

                datasetInfo = msDbg.dbsReader.getDBSSummaryInfo(wflow['tmp']['InputDataset'])
                totalLumis = datasetInfo['NumberOfLumis']
                totalEvents = datasetInfo['NumberOfEvents']
                totalFiles = datasetInfo['NumberOfFiles']
                lumisPerJob = math.ceil(wflow['tmp']['EventsPerJob']/wflow['tmp']['EventsPerLumi'])
                totalEstimatedJobs = math.ceil(totalEvents/(totalLumis/lumisPerJob)) # this number is not 100% correct but a fairly good initial estimate

                # Populate the so calculated values in the wflow tmp dictionary:
                wflow['tmp']['TotalInputEvents'] = totalEvents
                wflow['tmp']['TotalInputLumis'] = totalLumis
                wflow['tmp']['TotalEstimatedJobs'] = totalEstimatedJobs
                wflow['tmp']['TotalInputFiles'] = totalFiles
                logger.info('\n%s', pformat(wflow['tmp']))

            else:
                logger.info("Found a workflow with neither EventBased nor EventAwareLumiBased Splitting algorithm:")
                logger.info('\n%s', pformat(wflow['tmp']))
                continue

            # Append the workflow to the list of bad workflows
            badWfList.append(wflow)

    # Upload recalculated statistics:
    if not dryRunMode:
        for wflow in badWfList:
            if any([wflow['tmp'][key] is None for key in keySetToCheck]):
                logger.info("Skipping non fully recalculated stats for the following Workflow: \%s", pformat(wflow['tmp']))
                continue

            msDbg.reqmgr2.updateRequestStats(wflow['RequestName'],
                                             {'total_jobs':      wflow['tmp']['TotalEstimatedJobs'],
                                              'input_events':    wflow['tmp']['TotalInputEvents'],
                                              'input_num_files': wflow['tmp']['TotalInputFiles'],
                                              'input_lumis':     wflow['tmp']['TotalInputLumis']})

    # Print out only the fields of interest:
    # logger.info("\n%s", pformat([{key: wflow['tmp'][key] for key in keySetHighLevel | keySetToFetch | keySetToCheck} for wflow in badWfList]))

if __name__ == '__main__':

    # First parse the arguments:
    args = parseArgs()

    dryRunMode = strToBool(args.dry)

    if args.debug:
        logger = loggerSetup(logging.DEBUG)
    else:
        logger = loggerSetup()

    logger.info("Loading configFile: %s", args.config)
    config = loadConfigurationFile(args.config)
    msConfig = config.section_('views').section_('data').dictionary_()

    logger.info("Config file contents: %s", pformat(msConfig))
    logger.info("DryRunMode: %s", dryRunMode)

    # Call main:
    main()
