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
from hashlib import md5
try:
    from WMCore.Configuration import loadConfigurationFile
    from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
    from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
except ImportError as e:
    print("You do not have a proper environment (%s), please source the following:" % str(e))
    print("source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh")
    sys.exit(1)

# list of workflows in "acquired" status and existent in at least one WMAgent
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


def createNewId(doc):
    """
    Create a new document id to avoid conflicts with the missing
    documents. Which are missing and we can't even get their latest
    revision number...
    NOTE: same logic as in WorkQueueElement
    :param doc: the workqueue element doc
    :return: a new workqueue element doc
    """
    # Assume md5 is good enough for now
    myhash = md5()
    spacer = ';'  # character not present in any field
    myhash.update(doc['RequestName'] + spacer)
    # Task will be None in global inbox
    myhash.update(repr(doc['TaskName']) + spacer)
    myhash.update(",".join(sorted(doc['Inputs'].keys())) + spacer)
    # Check repr is reproducible - should be
    if doc['Mask']:
        myhash.update(",".join(["%s=%s" % (x, y) for x, y in doc['Mask'].items()]) + spacer)
    else:
        myhash.update("None" + spacer)
    # Check ACDC is deterministic and all params relevant
    myhash.update(",".join(["%s=%s" % (x, y) for x, y in doc['ACDC'].items()]) + spacer)
    myhash.update(repr(doc['Dbs']) + spacer)
    doc._id = myhash.hexdigest()
    doc.id = doc._id
    doc.pop("rev", None)
    return doc

def updateGlobalWQEs(config, listWflows):
    """
    Given a list of workflows, get their workqueue elements from the backup
    database and write those documents to CMSWEB workqueue.
    """
    print("\nGoing to update %d workflows in GLOBAL workqueue; they are: %s" % (len(listWflows), pformat(listWflows)))
    backend = WorkQueueBackend(config.WorkloadSummary.couchurl)
    # FIXME: my VM has the old database in place; URL hard-coded!!!
    backupBackend = WorkQueueBackend("https://alancc7-cloud1.cern.ch/couchdb")

    for wflow in listWflows:
        prodElems = backend.getElements(WorkflowName=wflow)
        print("Found %d GQEs in production for workflow: %s" % (len(prodElems), wflow))
        backupElems = backupBackend.getElements(WorkflowName=wflow)
        print("Found %d GQEs in backup for workflow: %s" % (len(backupElems), wflow))
        if prodElems and backupElems:
            print("NOT proceeding because there are elements in both prod and backup")
            continue
        elif not backupElems:
            print("NOT proceeding because there are NO elements in backup")
            continue

        # actually save those elements in production workqueue now
        for elem in backupElems:
            print("Couch element _id: %s, id: %s, rev: %s" % (elem._id, elem.id, elem.rev))
            elem = createNewId(elem)
            print("New element _id: %s, id: %s, rev: %s" % (elem._id, elem.id, elem.get("rev")))
        break
        res = backend.saveElements(*backupElems)
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
    parser.add_argument('-d', '--dry-run', dest="emulate", action='store_true',
                        help='Just emulate the whole thing without actually making write operations')
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

    if args.emulate:
        print("Script executed in dry-run mode, exiting...")
        sys.exit(0)

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

    updateGlobalWQEs(config, wfsToRecover)



if __name__ == '__main__':
    sys.exit(main())
