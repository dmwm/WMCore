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

try:
    from WMCore.Configuration import loadConfigurationFile
    from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
    from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
except ImportError as e:
    print("You do not have a proper environment (%s), please source the following:" % str(e))
    print("source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh")
    sys.exit(1)

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
    parser.add_argument('-d', '--dry-run', action='store_false',
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

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
