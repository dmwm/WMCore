#!/usr/bin/env python
"""
This script is meant to be used when you want to check the GQE/LQE status
in the agent and make sure that all elements are Done processing (or not).

Given a workflow name, it collects the status of the workqueue elements in:
 a) global workqueue
 b) local workqueue
 c) local workqueue_inbox
and prints a summary of them

NOTE: you need to source the agent environment:
source apps/wmagent/etc/profile.d/init.sh
"""
from __future__ import print_function, division

import os
import sys
from pprint import pprint

from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend


def createElementsSummary(reqName, elements, queueUrl):
    """
    Print the local couchdb situation based on the WQE status
    """
    print("\nSummary for %s and request %s" % (queueUrl, reqName))
    summary = {'numberOfElements': len(elements)}
    for elem in elements:
        summary.setdefault(elem['Status'], {})
        if elem['ChildQueueUrl'] not in summary[elem['Status']]:
            summary[elem['Status']][elem['ChildQueueUrl']] = 0
        summary[elem['Status']][elem['ChildQueueUrl']] += 1
    pprint(summary)


def main():
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if len(sys.argv) != 2:
        print("You must provide a request name")
        sys.exit(1)

    reqName = sys.argv[1]

    globalWQBackend = WorkQueueBackend(config.WorkloadSummary.couchurl, db_name="workqueue")
    localWQBackend = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name="workqueue")
    localWQInbox = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name="workqueue_inbox")

    gqDocIDs = globalWQBackend.getElements(RequestName=reqName)
    localDocIDs = localWQBackend.getElements(RequestName=reqName)
    localInboxDocIDs = localWQInbox.getElements(RequestName=reqName)

    createElementsSummary(reqName, gqDocIDs, globalWQBackend.queueUrl)
    createElementsSummary(reqName, localDocIDs, localWQBackend.queueUrl)
    createElementsSummary(reqName, localInboxDocIDs, localWQInbox.queueUrl)

    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())
