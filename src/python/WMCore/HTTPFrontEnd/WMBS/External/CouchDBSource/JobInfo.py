"""
JobInfoByID

Retrieve information about a job from couch and format it nicely.
"""

import sys
import datetime
import os
import time
import logging

from WMCore.HTTPFrontEnd.WMBS.External.CouchDBSource.CouchDBConnectionBase \
    import CouchDBConnectionBase

from WMCore.Database.CMSCouch import CouchError

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
    options = {"startkey": jobID, "endkey": jobID, "stale": "ok"}
    result = changeStateDB.loadView("JobDump",
                                    "stateTransitionsByJobID",
                                    options)

    for row in result["rows"]:
        if not transitionDocs.has_key(row["value"]["timestamp"]):
            transitionDocs[row["value"]["timestamp"]] = []

        transitionDocs[row["value"]["timestamp"]].append(row["value"])

    options = {"startkey": jobID, "endkey": jobID, "include_docs": True, "stale": "ok"}
    fwjrDocsResult = changeStateDB.loadView("JobDump", "fwjrsByJobID", options)
    jobDocResult = changeStateDB.loadView("JobDump", "jobsByJobID", options)

    if len(jobDocResult["rows"]) == 0:
        print "Unknown job: %s" % jobID
        sys.exit(1)
    elif len(jobDocResult["rows"]) > 1:
        print "Multiple entries for this job: %s" % jobID
        sys.exit(1)

    jobDoc = jobDocResult["rows"][0]["doc"]

    for row in fwjrDocsResult["rows"]:
        fwjrDocs[row["doc"]["retrycount"]] = row["doc"]["fwjr"]

    return {'jobDoc' : jobDoc}

def getJobSummaryByWorkflow(couchConfig):
    """
    gets the job status information by workflow

    example
    {"rows":[
        {"key":["MonteCarlo-v10"],"value":{"pending":11,"running":0,"cooloff":0,"success":0,"failure":0}},
        {"key":["MonteCarlo-v11"],"value":{"pending":22,"running":0,"cooloff":0,"success":0,"failure":0}},
        {"key":["MonteCarlo-v6"],"value":{"pending":1,"running":0,"cooloff":0,"success":0,"failure":0}},
        {"key":["MonteCarlo-v8"],"value":{"pending":7,"running":0,"cooloff":0,"success":0,"failure":0}}
     ]}
    """
    try:
        couchDBBase = CouchDBConnectionBase(couchConfig)
        changeStateDB = couchDBBase.getCouchJobsDB()
    except:
        #TODO log the error in the server
        #If the server is down it doesn't throw CouchError,
        #Need to distinquish between server down and CouchError
        return [{"error": 'Couch connection error'}]

    options = {"group": True, "group_level": 1, "stale": "ok"}
    result = changeStateDB.loadView("JobDump", "statusByWorkflowName",
                                    options)

    quotedJobsDBName = couchDBBase.getCouchDBName() + "%2Fjobs"
    quotedFWJRDBName = couchDBBase.getCouchDBName() + "%2Ffwjrs"

    couchDocBase = couchDBBase.getCouchDBHtmlBase(quotedFWJRDBName, "FWJRDump",
                                                  "workflowSummary")
    # reformat to match other type. (not very performative)
    formatted = []
    for item in result['rows']:
        dictItem = {}
        dictItem['request_name'] = item['key'][0]
        dictItem.update(item['value'])
        dictItem['couch_doc_base'] = "%s/%s" % (couchDocBase, item['key'][0])
        options = {'startkey':'["%s"]' % item['key'][0],
                   'endkey':'["%s",{}]' % item['key'][0],
                   "reduce": "false",
                   "stale": "ok"}

        dictItem['couch_job_info_base'] = couchDBBase.getCouchDBHtmlBase(quotedJobsDBName,
                                                                         "JobDump", "replace_to_Jobs",
                                                                         'statusByWorkflowName', options = options,
                                                                         type = "list")

        formatted.append(dictItem)

    return formatted

def getJobStateBySite(couchConfig):
    """
    report jobstatus by site within hour period
    only for complete jobs (complete, success, jobfailed states)
    """
    try:
        couchDBBase = CouchDBConnectionBase(couchConfig)
        changeStateDB = couchDBBase.getCouchJobsDB()
    except:
        #TODO log the error in the server
        #If the server is down it doesn't throw CouchError,
        #Need to distinquish between server down and CouchError
        return [{"error": 'Couch connection error'}]
    currentTime = int(time.time())
    startkey = [currentTime - (currentTime % 3600)]
    endkey = [currentTime, {}, {}]
    options = {"group_level": 3, "startkey": startkey, "endkey":endkey, "stale": "ok"}

    result = changeStateDB.loadView("JobDump", "hourlyStatusBySiteName", options)

    # reformat to match other type.
    formatted = []

    currentSite = None
    siteDict = None
    #result is sorted by site.
    for item in result['rows']:
        if item['key'][1] == None:
            logging.error("Site info is missing, Ignore data : %s" % item)
            continue
        if currentSite == item['key'][1]:
            siteDict[item['key'][2]] = item['value']
        else:
            if siteDict != None:
                formatted.append(siteDict)
            siteDict = {}
            siteDict['site_name'] = item['key'][1]
            siteDict[item['key'][2]] = item['value']
            currentSite = item['key'][1]

    if siteDict != None:
        formatted.append(siteDict)

    return formatted
