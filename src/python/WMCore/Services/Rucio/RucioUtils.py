#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module contains a few basic WMCore-related utilitarian
Rucio data structures and functions
"""

import random

RUCIO_VALID_PROJECT = ("Production", "RelVal", "Tier0", "Test", "User")
# grouping values are extracted from:
# https://github.com/rucio/rucio/blob/master/lib/rucio/common/schema/cms.py#L117
GROUPING_DSET = "DATASET"
GROUPING_ALL = "ALL"
RUCIO_RULES_PRIORITY = {"low": 2, "normal": 3, "high": 4, "reserved": 5}
# number of copies to be defined when creating replication rules
NUM_COPIES_DEFAULT = 1
NUM_COPIES_NANO = 2

def validateMetaData(did, metaDict, logger):
    """
    This function can be extended in the future, for now it will only
    validate the DID creation metadata, more specifically only the
    "project" parameter
    :param did: the DID that will be inserted
    :param metaDict: a dictionary with all the DID metadata data to be inserted
    :param logger: a logger object
    :return: False if validation fails, otherwise True
    """
    if metaDict.get("project", "Production") in RUCIO_VALID_PROJECT:
        return True
    msg = f"DID: {did} has an invalid 'project' meta-data value: {metaDict['project']}"
    msg += f"The supported 'project' values are: {str(RUCIO_VALID_PROJECT)}"
    logger.error(msg)
    return False


def weightedChoice(rses, rseWeights):
    """
    Given a list of items and their respective weights (quota in this case),
    perform a weighted selection.
    :param rses: a list of tuples with the RSE name and whether it requires approval
    :param rseWeights: a list with RSE weights (quota)
    :return: a tuple from the choices list
    """
    listChoice = random.choices(population=rses, weights=rseWeights, k=1)
    # return only the tuple, not the list with a tuple item
    return listChoice[0]


def isTapeRSE(rseName):
    """
    Given an RSE name, return True if it's a Tape RSE (rse_type=TAPE), otherwise False
    :param rseName: string with the RSE name
    :return: True or False
    """
    # NOTE: a more reliable - but more expensive - way to know that would be
    # to query `get_rse` and evaluate the rse_type parameter
    return rseName.endswith("_Tape")


def dropTapeRSEs(listRSEs):
    """
    Method to parse a list of RSE names and return only those that
    are not a rse_type=TAPE, so in general only Disk endpoints
    :param listRSEs: list with the RSE names
    :return: a new list with only DISK RSE names
    """
    diskRSEs = []
    for rse in listRSEs:
        if isTapeRSE(rse):
            continue
        diskRSEs.append(rse)
    return diskRSEs
