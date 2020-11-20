#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to be executed with the WMAgent environment, does you first need to execute:
source apps/wmagent/etc/profile.d/init.sh

Provided a workflow name in the command line, it will find all the
local workqueue elements and print a summary of work/data location
for the elements sitting in the Available status
"""
from __future__ import print_function, division

import sys
import os

from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
from WMCore.WorkQueue.DataStructs.WorkQueueElement import possibleSites


def printElementsSummary(reqName, elements, queueUrl):
    """
    Print the local couchdb situation based on the WQE status
    """
    print("Summary for %s and request %s" % (queueUrl, reqName))
    for elem in elements:
        if elem['Status'] != "Available":
            continue
        targetSites = possibleSites(elem)
        commonDataLoc = commonDataLocation(elem)
        print("  Element '%s' has the following site intersection: %s, with common data location: %s"
              % (elem.id, targetSites, commonDataLoc))
        printDataLocation(elem)

        if not targetSites and commonDataLoc:
            print("    this workflow has to be assigned to: %s" % commonDataLoc)
        if not targetSites and not commonDataLoc:
            print("    this workflow has to be assigned with AAA flags enabled according to input/PU location")


def printDataLocation(element):
    """
    Print all the input data and their current location
    """
    print("    LQE has primary AAA: %s and secondary AAA: %s" % (element['NoInputUpdate'], element['NoPileupUpdate']))
    print("    LQE has Inputs: %s" % element['Inputs'])
    print("    LQE has ParentData: %s" % element['ParentData'])
    print("    LQE has PileupData: %s" % element['PileupData'])

def commonDataLocation(element):
    """
    Make an intersection of all the data location
    :param element: workqueue element object
    :return: list with the common **data** location
    """
    commonLoc = set()
    if element['PileupData']:
        commonLoc = set(element['PileupData'].values()[0])
    if element['Inputs']:
        if commonLoc:
            commonLoc = commonLoc & set(element['Inputs'].values()[0])
        else:
            commonLoc = set(element['Inputs'].values()[0])
    if element['ParentData']:
        tempLoc = element['ParentData'].values()
        parentLoc = set(tempLoc[0])
        for temp in tempLoc:
            parentLoc = parentLoc & set(temp)
        commonLoc = commonLoc & parentLoc
    return commonLoc

def main():
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if len(sys.argv) != 2:
        print("You must provide a request name")
        sys.exit(1)
    reqName = sys.argv[1]

    localWQBackend = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name="workqueue")
    localDocIDs = localWQBackend.getElements(WorkflowName=reqName)
    printElementsSummary(reqName, localDocIDs, localWQBackend.queueUrl)

    sys.exit(0)

if __name__ == '__main__':
    sys.exit(main())
