"""
JobInfoByID

Retrieve information about a job from couch and format it nicely.
"""
from __future__ import print_function

import sys
import datetime
import os

from WMCore.HTTPFrontEnd.WMBS.External.CouchDBSource.CouchDBConnectionBase \
    import CouchDBConnectionBase

def getJobInfo(jobID, couchConfig):
    """
    _getJobInfo_

    Retrieve all the job metadata out of couch.
    """
    jobID = int(jobID)
    couchDBBase = CouchDBConnectionBase(couchConfig)
    changeStateDB = couchDBBase.getCouchDB()

    jobDoc = {}
    fwjrDocs = {}
    transitionDocs = {}
    options = {"startkey": jobID, "endkey": jobID}
    result = changeStateDB.loadView("JobDump",
                                    "stateTransitionsByJobID",
                                    options)

    for row in result["rows"]:
        if row["value"]["timestamp"] not in transitionDocs:
            transitionDocs[row["value"]["timestamp"]] = []

        transitionDocs[row["value"]["timestamp"]].append(row["value"])

    options = {"startkey": jobID, "endkey": jobID, "include_docs": True}
    fwjrDocsResult = changeStateDB.loadView("JobDump", "fwjrsByJobID", options)
    jobDocResult = changeStateDB.loadView("JobDump", "jobsByJobID", options)

    if len(jobDocResult["rows"]) == 0:
        print("Unknown job: %s" % jobID)
        sys.exit(1)
    elif len(jobDocResult["rows"]) > 1:
        print("Multiple entries for this job: %s" % jobID)
        sys.exit(1)

    jobDoc = jobDocResult["rows"][0]["doc"]

    for row in fwjrDocsResult["rows"]:
        fwjrDocs[row["doc"]["retrycount"]] = row["doc"]["fwjr"]

    return {'jobDoc' : jobDoc}
