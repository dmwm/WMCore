#!/usr/bin/env python
"""
Script meant to fetch the number of blocks and files for a given dataset,
using both DBS and Rucio services. It prints any inconsistency among
those two.
"""
from __future__ import print_function, division

import logging
import sys
from collections import Counter

from future.utils import viewkeys, viewvalues

from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.Rucio.Rucio import Rucio

RUCIO_ACCT = "wma_prod"
RUCIO_HOST = "http://cms-rucio.cern.ch"
RUCIO_AUTH = "https://cms-rucio-auth.cern.ch"
DBS_URL = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"


def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    logger = logging.getLogger(__name__)
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger


def getFromRucio(dataset, logger):
    """
    Using the WMCore Rucio object and fetch all the blocks and files
    for a given container.
    Returns a dictionary key'ed by the block name, value is the amount of files.
    """
    rucio = Rucio(acct=RUCIO_ACCT,
                  hostUrl=RUCIO_HOST,
                  authUrl=RUCIO_AUTH,
                  configDict={'logger': logger})

    result = dict()
    for block in rucio.getBlocksInContainer(dataset):
        data = rucio.getDID(block)
        result.setdefault(block, data['length'])
    return result


def getFromDBS(dataset, logger):
    """
    Uses the WMCore DBS3Reader object to fetch all the blocks and files
    for a given container.
    Returns a dictionary key'ed by the block name, and an inner dictionary
    with the number of valid and invalid files. It also returns a total counter
    for the number of valid and invalid files in the dataset.
    """
    dbsReader = DBS3Reader(DBS_URL, logger)

    result = dict()
    dbsFilesCounter = Counter({'valid': 0, 'invalid': 0})
    blocks = dbsReader.listFileBlocks(dataset)
    for block in blocks:
        data = dbsReader.dbs.listFileArray(block_name=block, validFileOnly=0, detail=True)
        result.setdefault(block, Counter({'valid': 0, 'invalid': 0}))
        for fileInfo in data:
            if fileInfo['is_file_valid'] == 1:
                result[block]['valid'] += 1
                dbsFilesCounter['valid'] += 1
            else:
                result[block]['invalid'] += 1
                dbsFilesCounter['invalid'] += 1
    return result, dbsFilesCounter


def main():
    """
    Expects a dataset name as input argument.
    It then queries Rucio and DBS and compare their blocks and
    number of files.
    """
    if len(sys.argv) != 2:
        print("A dataset name must be provided in the command line")
        sys.exit(1)
    datasetName = sys.argv[1]

    logger = loggerSetup(logging.INFO)

    rucioOutput = getFromRucio(datasetName, logger)
    dbsOutput, dbsFilesCounter = getFromDBS(datasetName, logger)

    logger.info("*** Dataset: %s", datasetName)
    logger.info("Rucio file count : %s", sum(viewvalues(rucioOutput)))
    logger.info("DBS file count   : %s", dbsFilesCounter['valid'] + dbsFilesCounter['invalid'])
    logger.info(" - valid files   : %s", dbsFilesCounter['valid'])
    logger.info(" - invalid files : %s", dbsFilesCounter['invalid'])
    logger.info("Blocks in Rucio but not in DBS: %s", set(viewkeys(rucioOutput)) - set(viewkeys(dbsOutput)))
    logger.info("Blocks in DBS but not in Rucio: %s", set(viewkeys(dbsOutput)) - set(viewkeys(rucioOutput)))

    for blockname in rucioOutput:
        if blockname not in dbsOutput:
            logger.error("This block does not exist in DBS: %s", blockname)
            continue
        if rucioOutput[blockname] != sum(viewvalues(dbsOutput[blockname])):
            logger.warning("Block with file mismatch: %s", blockname)
            logger.warning("\tRucio: %s\t\tDBS: %s", rucioOutput[blockname], sum(viewvalues(dbsOutput[blockname])))


if __name__ == "__main__":
    sys.exit(main())
