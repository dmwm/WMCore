#!/usr/bin/env python
"""
Script to get requests from ReqMgr2, in a given status, and compare
those workflow names against what's known by the local workqueue.
The workqueue query includes workqueue, workqueue_inbox and spec
 """
from __future__ import print_function

import argparse
import os
import sys
from pprint import pformat

try:
    from WMCore.Algorithms.MiscAlgos import dict_diff
    from WMCore.Configuration import loadConfigurationFile
    from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
    from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
    from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement
    from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement
except ImportError as e:
    print("You do not have a proper environment (%s), please source the following:" % str(e))
    print("source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh")
    sys.exit(1)

# FIXME: list of workflows in "acquired" status and existent in at least one WMAgent
# I just know it because I went to every single agent and queried its local queue..
COMMON = {'pdmvserv_task_B2G-RunIIFall17NanoAODv6-01555__v1_T_191127_115756_7429',
          'pdmvserv_task_HIG-RunIIFall17NanoAODv6-02954__v1_T_191127_144106_9125',
          'pdmvserv_task_HIG-RunIIFall17NanoAODv6-02606__v1_T_191127_121513_4999',
          'pdmvserv_task_HIG-RunIIAutumn18NanoAODv6-02064__v1_T_191127_145611_2553',
          'pdmvserv_task_EXO-RunIISummer16NanoAODv6-06080__v1_T_191109_074711_9969',
          'pdmvserv_task_HIG-RunIIFall17NanoAODv6-02407__v1_T_191127_114220_7869',
          'pdmvserv_task_EXO-RunIIFall17NanoAODv6-03019__v1_T_191127_180201_5820',
          'pdmvserv_task_HIG-RunIIFall17NanoAODv6-03253__v1_T_191127_181904_7533',
          'pdmvserv_task_SUS-RunIIFall17NanoAODv6-00265__v1_T_191127_182125_8992',
          'pdmvserv_task_EXO-RunIIFall17NanoAODv6-03115__v1_T_191127_215655_4912',
          'pdmvserv_task_HIG-RunIIFall17NanoAODv6-02926__v1_T_191127_142922_1970',
          'pdmvserv_task_B2G-RunIIFall17NanoAODv6-01577__v1_T_191127_141837_3089',
          'pdmvserv_task_B2G-RunIIAutumn18NanoAODv6-01692__v1_T_191120_134423_6009',
          'pdmvserv_task_EXO-RunIIFall17NanoAODv6-00079__v1_T_191122_134805_8704'}


def newQueueElement(doc):
    """
    Create a new workqueue element using the data from the backup document
    """
    args = {}
    # parameters provided to newQueueElement
    args['Inputs'] = doc['Inputs']
    args['ParentFlag'] = doc['ParentFlag']
    args['ParentData'] = doc['ParentData']
    args['NumberOfLumis'] = doc['NumberOfLumis']
    args['NumberOfFiles'] = doc['NumberOfFiles']
    args['NumberOfEvents'] = doc['NumberOfEvents']
    args['Jobs'] = doc['Jobs']
    args['OpenForNewData'] = doc['OpenForNewData']
    args['NoInputUpdate'] = doc['NoInputUpdate']
    args['NoPileupUpdate'] = doc['NoPileupUpdate']

    # others added by newQueueElement method
    args['Status'] = doc['Status']
    args['WMSpec'] = doc['WMSpec']
    args['Task'] = doc['Task']
    args['RequestName'] = doc['RequestName']
    args['TaskName'] = doc['TaskName']
    args['Dbs'] = doc['Dbs']
    args['SiteWhitelist'] = doc['SiteWhitelist']
    args['SiteBlacklist'] = doc['SiteBlacklist']
    args['StartPolicy'] = doc['StartPolicy']
    args['EndPolicy'] = doc['EndPolicy']
    args['Priority'] = doc['Priority']
    args['PileupData'] = doc['PileupData']
    args['Priority'] = doc['Priority']

    # others added in a different way
    args['Mask'] = doc['Mask']
    args['blowupFactor'] = doc['blowupFactor']
    args['ParentQueueId'] = doc['ParentQueueId']
    args['CreationTime'] = doc['CreationTime']
    args['TeamName'] = doc['TeamName']

    ele = WorkQueueElement(**args)
    return ele


def insertElements(backendObj, units):
    """
    Given a WorkQueueBackend object, use it to insert workqueue
    elements against the workqueue global database
    """
    if not units:
        print("  Nothing to insert!")
        return

    newUnitsInserted = []
    for unit in units:
        # cast to couch
        if not isinstance(unit, CouchWorkQueueElement):
            unit = CouchWorkQueueElement(backendObj.db, elementParams=dict(unit))

        if unit._couch.documentExists(unit.id):
            print('  Element "%s" already exists, skip insertion.' % unit.id)
            continue
        else:
            newUnitsInserted.append(unit)
        unit.save()
        unit._couch.commit(all_or_nothing=True)

    return newUnitsInserted


def updateGlobalWQEs(config, listWflows, emulate):
    """
    Given a list of workflows, get their workqueue elements from the backup
    database and write those documents to CMSWEB workqueue.
    """
    print("\nGoing to update %d workflows in GLOBAL workqueue; they are:\n%s" % (len(listWflows), pformat(listWflows)))
    backend = WorkQueueBackend(config.WorkloadSummary.couchurl)
    # FIXME: my VM has the old database in place; URL hard-coded!!!
    backupBackend = WorkQueueBackend("https://alancc7-cloud1.cern.ch/couchdb")

    for wflow in listWflows:
        prodElems = backend.getElements(WorkflowName=wflow)
        backupElems = backupBackend.getElements(WorkflowName=wflow)
        print("There are %d production and %d backup GQEs for workflow: %s" % (len(prodElems), len(backupElems), wflow))

        if not backupElems:
            print("  NOT proceeding! No backup elements")
            continue
        elif len(prodElems) == len(backupElems):
            print("  NOT proceeding! Backup and production are already in sync!")
            continue

        newElements = []
        for elem in backupElems:
            newElem = newQueueElement(elem)
            if dict_diff(elem, newElem):
                print("BUG!!! Backup and new element differ: %s" % dict_diff(elem, newElem))
                newElements = []
                break
            newElements.append(newQueueElement(elem))

        if emulate:
            print("Not writing anything because it's running in dry-run mode!!!")
            break
        res = insertElements(backend, newElements)
        print("Result for %s is: %s\n" % (wflow, res))
        break

    return


def checkLocalWQStatus(config):
    """
    Given a WorkQueueElement status, query local workqueue and workqueue_inbox
    database for all elements in a given status and that were acquired by this agent.
    """
    backend = WorkQueueBackend(config.WorkQueueManager.couchurl)
    wfs = backend.getWorkflows(includeInbox=True, includeSpecs=True)
    return wfs


def getWfsByStatus(config, status):
    """
    Fetch workflows in given status in ReqMgr2
    """
    if isinstance(status, basestring):
        status = [status]

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL,
                                 couchapp=config.AnalyticsDataCollector.RequestCouchApp)
    wfs = wfDBReader.getRequestByStatus(status, detail=False)

    return wfs


def parseArgs():
    """
    Well, parse the arguments passed in the command line :)
    """
    parser = argparse.ArgumentParser(description="Does a bunch of draining checks")
    parser.add_argument('-s', '--status', action='store', dest="status", required=True,
                        help='Status of workflow in ReqMgr2')
    parser.add_argument('-w', '--write-docs', dest="writeDocs", action='store_true',
                        help='Set it if you want to perform write operations')
    args = parser.parse_args()
    return args


def main():
    args = parseArgs()

    os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    reqmgrWfs = getWfsByStatus(config, args.status)
    print("Found %d workflows in ReqMgr2 in status: %s" % (len(reqmgrWfs), args.status))

    localWfs = checkLocalWQStatus(config)
    print("Found %d workflows in Local WorkQueue" % len(localWfs))

    common = set(reqmgrWfs) & set(localWfs)
    print("Found %d common workflows between LQ and ReqMgr2. They are: %s" % (len(common), common))

    wfsToRecover = []
    counter = 0
    for wf in reqmgrWfs:
        if wf in localWfs:
            print("Skipping workflow %s found in local workqueue" % wf)
            continue
        elif wf in COMMON:
            print("Skipping workflow %s found in COMMON workflows" % wf)
            continue
        elif "task_HIG-RunIISummer15wmLHEGS" in wf:
            wfsToRecover.append(wf)
            counter += 1
        if counter >= 3:
            break

    updateGlobalWQEs(config, wfsToRecover, not args.writeDocs)


if __name__ == '__main__':
    sys.exit(main())
