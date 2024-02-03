"""
This module provides some functions to create concurrent HTTP requests
for CouchDB, as an alternative to the sequential modules available under:
* WMCore/Services/WMStats/WMStatsReader.getTaskJobSummaryByRequest
* WMCore/Services/WMStats/WMStatsReader.jobDetailByTasks

Documentation for the parameters supported in views query can be found at:
https://docs.couchdb.org/en/stable/api/ddoc/views.html#get--db-_design-ddoc-_view-view
"""
import json

from urllib.parse import urljoin, urlencode
from Utils.CertTools import ckey, cert
from WMCore.Services.pycurl_manager import getdata as multi_getdata


def getTaskJobSummaryByRequestPycurl(rowsSummary, sampleSize, serviceOpts):
    """
    Pycurl-based implementation of WMStatsReader.getTaskJobSummaryByRequest
    :param rowsSummary: a dictionary with rows from CouchDB
    :param sampleSize: integer with number of documents to retrieve
    :param serviceOpts: dictionary with CouchDB options (url, db name and couchapp name)
    :return: a dictionary with job detail
    """
    paramQueries = []
    for row in rowsSummary['rows']:
        thisQuery = {"startkey": [], "endkey": [], "numOfError": 0}
        # row["key"] = ['workflow', 'task', 'jobstatus', 'exitCode', 'site']
        thisQuery["startkey"] = row["key"][:4]
        if row["key"][4]:
            thisQuery["startkey"].append(row["key"][4])  # site

        thisQuery["endkey"] = []
        thisQuery["endkey"].extend(thisQuery["startkey"])
        thisQuery["endkey"].append({})
        # append amount of errors matching the 5 keys above
        thisQuery["numOfError"] = row["value"]
        # add query to the pool
        paramQueries.append(thisQuery)

    return jobDetailByTasksPycurl(paramQueries, sampleSize, serviceOpts)


def jobDetailByTasksPycurl(queries, limit, serviceOpts):
    """
    Pycurl-based implementation of WMStatsReader.jobDetailByTasks.
    In short, for each tuple of errors, it returns "limit" number of documents.
    :param queries: list of CouchDB query parameters
    :param limit: number of documents to retrieve from CouchDB
    :param serviceOpts: dictionary with CouchDB options (url, db name and couchapp name)
    :return: dictionary with job detail information, in a format like:
        {"WORKFLOW_NAME":
            {"/WORKFLOW_NAME/TASK_NAME":
                {"JOB_STATE":
                    {"EXIT_CODE":
                        {"SITE_NAME":
                            "samples": [
                                {"_id": "123abc",
                                 "_rev": "12-abc",
                                 "wmbsid": 2006208,
                                 "type": "jobsummary",
                                 "retrycount": 3,
                                 "errors": {"JobSubmit": [{
                                    "type": "SubmitFailed",
                                    "details": "The job can blah blah",
                                    "exitCode": 71103}]
                                    },
                                "timestamp": 1698749157,
                                ... etc etc}],
                            "errorCount": 1}
                        }
                    }
                },
            {"/WORKFLOW_NAME/TASK_NAME-2":
                  {"JOB_STATE": {... etc etc

    A decoded example of this query would be:
        scurl "https://cmsweb-test9.cern.ch/couchdb/wmstats/_design/WMStatsErl3/_view/jobsByStatusWorkflow?reduce=false&include_docs=true&startkey=["WORKFLOW_NAME","/WORKFLOW_NAME/TASK_NAME","JOB_STATE",EXIT_CODE,"SITE_NAME"]&endkey=["WORKFLOW_NAME","/WORKFLOW_NAME/TASK_NAME","JOB_STATE",EXIT_CODE,"SITE_NAME",{}]&limit=1&stale=update_after"
    while encoding the url would result in:
        scurl "https://****.cern.ch/couchdb/wmstats/_design/WMStatsErl3/_view/jobsByStatusWorkflow?reduce=false&include_docs=true&startkey=%5B%22WORKFLOW_NAME%22%2C+%22%2FWORKFLOW_NAME%2FTASK_NAME%22%2C+%22JOB_STATE%22%2C+EXIT_CODE%2C+%22SITE_NAME%22%5D&endkey=%5B%22WORKFLOW_NAME%22%2C+%22%2FWORKFLOW_NAME%2FTASK_NAME%22%2C+%22JOB_STATE%22%2C+EXIT_CODE%2C+%22SITE_NAME%22%2C+%7B%7D%5D&limit=1&stale=update_after"

    Example of output is:
        {"total_rows":4764135,"offset":4056263,"rows":[
        {"id":"12703ce5-xxx","key":["WORKFLOW_NAME","/WORKFLOW_NAME/TASK_NAME","jobfailed",8006,"SITE_NAME","https://xxx.cern.ch/couchdb/acdcserver","vocms0255.cern.ch",["Fatal Exception"]],
         "value":{"id":"12703ce5-xxx","rev":"4-xxx"},
         "doc":{"_id":"12703ce5-xx","_rev":"4-xxx","wmbsid":2709786,"type":"jobsummary", ...

    where (for the examples above):
        WORKFLOW_NAME is, e.g.: pdmvserv_Run2017G_DoubleMuon_UL2017_MiniAODv2_BParking_230917_124108_9876
        TASK_NAME is, e.g.: DataProcessing
        SITE_NAME is, e.g.: T2_US_Wisconsin
        JOB_STATE is, e.g.: jobfailed
        EXIT_CODE is, e.g.: 8006
    """
    uri = f"couchdb/{serviceOpts['dbName']}/_design/{serviceOpts['couchapp']}/_view/jobsByStatusWorkflow"
    baseUrl = urljoin(serviceOpts['couchURL'], uri)

    encoder = json.JSONEncoder()
    urlsPool = []
    for query in queries:
        options = {'include_docs': encoder.encode(True),
                   'reduce': encoder.encode(False),
                   'startkey': encoder.encode(query["startkey"]),
                   'endkey': encoder.encode(query["endkey"]),
                   'limit': encoder.encode(limit)}
        # we cannot encode the 'stale' parameter
        options.setdefault("stale", "update_after")
        # encode url data for the GET request
        thisUrl = f"{baseUrl}?{urlencode(options, doseq=True)}"
        urlsPool.append(thisUrl)

    jobInfoDoc = {}
    # now run all of these calls in parallel
    for response in multi_getdata(urlsPool, ckey(), cert()):
        if 'error' in response:
            raise RuntimeError(f"Unexpected error in HTTP call. Details: {response}")
        data = json.loads(response.get('data', ''))
        if 'error' in data:
            raise RuntimeError(f"CouchDB query failed. Details: {data}")

        for row in data.get('rows', []):
            keys = row['key']
            workflow = keys[0]
            task = keys[1]
            jobStatus = keys[2]
            exitCode = keys[3]
            site = keys[4]

            jobInfoDoc.setdefault(workflow, {})
            jobInfoDoc[workflow].setdefault(task, {})
            jobInfoDoc[workflow][task].setdefault(jobStatus, {})
            jobInfoDoc[workflow][task][jobStatus].setdefault(exitCode, {})
            jobInfoDoc[workflow][task][jobStatus][exitCode].setdefault(site, {})
            finalStruct = jobInfoDoc[workflow][task][jobStatus][exitCode][site]
            finalStruct.setdefault("samples", [])
            finalStruct["samples"].append(row["doc"])
            # now painfully find out the number of errors based on the original query
            finalStruct.setdefault("errorCount", 0)
            keysJson = json.dumps(keys[:5])
            for query in queries:
                if json.dumps(query["startkey"]) in keysJson:
                    finalStruct["errorCount"] = query["numOfError"]
                    break

    return jobInfoDoc
