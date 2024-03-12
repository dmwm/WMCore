from __future__ import division, print_function

import time

from builtins import object
from future.utils import viewitems

import logging

from Utils.IteratorTools import nestedDictUpdate, grouper
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import RequestInfo
from WMCore.ReqMgr.DataStructs.RequestStatus import T0_ACTIVE_STATUS
from WMCore.Services.WMStats.WMStatsPycurl import getTaskJobSummaryByRequestPycurl


REQUEST_PROPERTY_MAP = {
    "_id": "_id",
    "InputDataset": "inputdataset",
    "PrepID": "prep_id",
    "Group": "group",
    "RequestDate": "request_date",
    "Campaign": "campaign",
    "RequestName": "workflow",
    "RequestorDN": "user_dn",
    "RequestPriority": "priority",
    "Requestor": "requestor",
    "RequestType": "request_type",
    "DbsUrl": "dbs_url",
    "CMSSWVersion": "cmssw",
    "OutputDatasets": "outputdatasets",
    "RequestTransition": "request_status",  # Status: status,  UpdateTime: update_time
    "SiteWhitelist": "site_white_list",
    "Team": "team",
    "TotalEstimatedJobs": "total_jobs",
    "TotalInputEvents": "input_events",
    "TotalInputLumis": "input_lumis",
    "TotalInputFiles": "input_num_files",
    "Run": "run",
    # Status and UpdateTime is under "RequestTransition"
    "Status": "status",
    "UpdateTime": "update_time"
}


def convertToLegacyFormat(requestDoc):
    converted = {}
    for key, value in viewitems(requestDoc):

        if key == "RequestTransition":
            newValue = []
            for transDict in value:
                newItem = {}
                for transKey, transValue in viewitems(transDict):
                    newItem[REQUEST_PROPERTY_MAP.get(transKey, transKey)] = transValue
                    newValue.append(newItem)
            value = newValue

        converted[REQUEST_PROPERTY_MAP.get(key, key)] = value

    return converted


class WMStatsReader(object):

    def __init__(self, couchURL, appName="WMStats", reqdbURL=None,
                 reqdbCouchApp="ReqMgr", logger=None):
        self._sanitizeURL(couchURL)
        # set the connection for local couchDB call
        self._commonInit(couchURL, appName)
        if reqdbURL:
            self.reqDB = RequestDBReader(reqdbURL, reqdbCouchApp)
        else:
            self.reqDB = None
        self.logger = logger if logger else logging.getLogger()
        self.serviceOpts = {"couchURL": self.couchURL,
                            "dbName": self.dbName,
                            "couchapp": self.couchapp}

    def _sanitizeURL(self, couchURL):
        return sanitizeURL(couchURL)['url']

    def _commonInit(self, couchURL, appName="WMStats"):
        """
        setting up comon variables for inherited class.
        inherited class should call this in their init function
        """

        self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchServer = CouchServer(self.couchURL)
        self.couchDB = self.couchServer.connectDatabase(self.dbName, False)
        self.couchapp = appName
        self.defaultStale = {"stale": "update_after"}

    def setDefaultStaleOptions(self, options):
        if not options:
            options = {}
        if 'stale' not in options:
            options.update(self.defaultStale)
        return options

    def getLatestJobInfoByRequests(self, requestNames):
        jobInfoByRequestAndAgent = {}

        if len(requestNames) > 0:
            requestAndAgentKey = self._getRequestAndAgent(requestNames)
            jobInfoByRequestAndAgent = self._getLatestJobInfo(requestAndAgentKey)
        return jobInfoByRequestAndAgent

    def _updateRequestInfoWithJobInfo(self, requestInfo):
        if requestInfo:
            jobInfoByRequestAndAgent = self.getLatestJobInfoByRequests(list(requestInfo))
            self._combineRequestAndJobData(requestInfo, jobInfoByRequestAndAgent)

    def _getCouchView(self, view, options, keys=None):
        keys = keys or []
        options = self.setDefaultStaleOptions(options)

        if keys and isinstance(keys, str):
            keys = [keys]
        return self.couchDB.loadView(self.couchapp, view, options, keys)

    def _formatCouchData(self, data, key="id"):
        result = {}
        for row in data['rows']:
            if 'error' in row:
                continue
            if "doc" in row:
                result[row[key]] = row["doc"]
            else:
                result[row[key]] = None
        return result

    def _combineRequestAndJobData(self, requestData, jobData):
        """
        update the request data with job info
        requestData['AgentJobInfo'] = {'vocms234.cern.ch:9999': {"_id":"d1d11dfcb30e0ab47db42007cb6fb847",
        "_rev":"1-8abfaa2de822ed081cb8d174e3e2c003",
        "status":{"inWMBS":334,"success":381,"submitted":{"retry":2,"pending":2},"failure":{"exception":3}},
        "agent_team":"testbed-integration","workflow":"amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731",
        "timestamp":1394738860,"sites":{"T2_CH_CERN_AI":{"submitted":{"retry":1,"pending":1}},
        "T2_CH_CERN":{"success":6,"submitted":{"retry":1,"pending":1}},
        "T2_DE_DESY":{"failure":{"exception":3},"success":375}},
        "agent":"WMAgent",
        "tasks":
           {"/amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731/Production":
            {"status":{"failure":{"exception":3},"success":331},
             "sites":{"T2_DE_DESY": {"success":325,"wrappedTotalJobTime":11305908,
                                     "dataset":{},"failure":{"exception":3},
                                     "cmsRunCPUPerformance":{"totalJobCPU":10869688.8,
                                                             "totalEventCPU":10832426.7,
                                                             "totalJobTime":11255865.9},
                                     "inputEvents":0},
                      "T2_CH_CERN":{"success":6,"wrappedTotalJobTime":176573,
                                    "dataset":{},
                                    "cmsRunCPUPerformance":{"totalJobCPU":167324.8,
                                                            "totalEventCPU":166652.1,
                                                            "totalJobTime":174975.7},
                                    "inputEvents":0}},
             "subscription_status":{"updated":1393108089, "finished":2, "total":2,"open":0},
             "jobtype":"Production"},
            "/amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731/Production/ProductionMergeRAWSIMoutput/ProductionRAWSIMoutputMergeLogCollect":
             {"jobtype":"LogCollect",
              "subscription_status":{"updated":1392885768,
              "finished":0,
              "total":1,"open":1}},
            "/amaltaro_OracleUpgrade_TEST_HG1401_140220_090116_6731/Production/ProductionMergeRAWSIMoutput":
              {"status":{"success":41,"submitted":{"retry":1,"pending":1}},
                "sites":{"T2_DE_DESY":{"datasetStat":{"totalLumis":973,"events":97300,"size":105698406915},
                                       "success":41,"wrappedTotalJobTime":9190,
                                       "dataset":{"/GluGluToHTohhTo4B_mH-350_mh-125_8TeV-pythia6-tauola/Summer12-OracleUpgrade_TEST_ALAN_HG1401-v1/GEN-SIM":
                                                   {"totalLumis":973,"events":97300,"size":105698406915}},
                                       "cmsRunCPUPerformance":{"totalJobCPU":548.92532,"totalEventCPU":27.449808,"totalJobTime":2909.92125},
                                    "inputEvents":97300},
                         "T2_CH_CERN":{"submitted":{"retry":1,"pending":1}}},
                "subscription_status":{"updated":1392885768,"finished":0,"total":1,"open":1},
                "jobtype":"Merge"},
           "agent_url":"vocms231.cern.ch:9999",
           "type":"agent_request"}}
        """
        if jobData:
            for row in jobData["rows"]:
                # condition checks if documents are deleted between calls.
                # just ignore in that case
                if row["doc"]:
                    jobInfo = requestData[row["doc"]["workflow"]]
                    jobInfo.setdefault("AgentJobInfo", {})
                    jobInfo["AgentJobInfo"][row["doc"]["agent_url"]] = row["doc"]

    def _getRequestAndAgent(self, filterRequest=None):
        """
        returns the [['request_name', 'agent_url'], ....]
        """
        options = {}
        options["reduce"] = True
        options["group"] = True
        result = self._getCouchView("requestAgentUrl", options)

        if filterRequest is None:
            keys = [row['key'] for row in result["rows"]]
        else:
            keys = [row['key'] for row in result["rows"] if row['key'][0] in filterRequest]
        return keys

    def _getLatestJobInfo(self, keys):
        """
        Given a list of lists as keys, in the format of:
            [['request_name', 'agent_url'], ['request_name2', 'agent_url2'], ....]
        The result format from the latestRequest view is:
            {u'offset': 527,
             u'rows': [{u'doc': {u'_rev': u'32-6027014210',
             ...
                        u'id': u'cmsgwms-submit6.fnal.gov-cmsunified_ACDC0_task_BTV-RunIISummer19UL18wmLHEGEN-00004__v1_T_200507_162125_3670',
                        u'key': [u'cmsunified_ACDC0_task_BTV-RunIISummer19UL18wmLHEGEN-00004__v1_T_200507_162125_3670',
                                 u'cmsgwms-submit6.fnal.gov'],
                        u'value': None}],
             u'total_rows': 49606}
        """
        if not keys:
            return []
        options = {}
        options["include_docs"] = True
        options["reduce"] = False
        finalResults = {}
        # magic number: 5000 keys (need to check which number is optimal)
        for sliceKeys in grouper(keys, 5000):
            self.logger.info("Querying latestRequest with %d keys", len(sliceKeys))
            result = self._getCouchView("latestRequest", options, sliceKeys)
            if not finalResults and result:
                finalResults = result
            elif result.get('rows'):
                finalResults['rows'].extend(result['rows'])
        return finalResults

    def _getAllDocsByIDs(self, ids, include_docs=True):
        """
        keys is [id, ....]
        returns document
        """
        if len(ids) == 0:
            return None
        options = {}
        options["include_docs"] = include_docs
        result = self.couchDB.allDocs(options, ids)

        return result

    def _getAgentInfo(self):
        """
        returns all the agents status on wmstats
        """
        options = {}
        result = self._getCouchView("agentInfo", options)

        return result

    def agentsByTeam(self, filterDrain=False):
        """
        return a dictionary like {team:#agents,...}
        """
        result = self._getAgentInfo()
        response = dict()

        for agentInfo in result["rows"]:
            #filtering empty string
            team = agentInfo['value']['agent_team']
            if not team:
                continue

            response.setdefault(team, 0)
            if filterDrain:
                if not agentInfo['value'].get('drain_mode', False):
                    response[team] += 1
            else:
                response[team] += 1

        return response

    def getServerInstance(self):
        return self.couchServer

    def getDBInstance(self):
        return self.couchDB

    def getRequestDBInstance(self):
        return self.reqDB

    def getHeartbeat(self):
        try:
            return self.couchDB.info()
        except Exception as ex:
            return {'error_message': str(ex)}

    def getRequestByNames(self, requestNames, jobInfoFlag=False):
        """
        To use this function reqDBURL need to be set when wmstats initialized.
        This will be deplicated so please don use this.
        """
        requestInfo = self.reqDB.getRequestByNames(requestNames, True)

        if jobInfoFlag:
            # get request and agent info
            self._updateRequestInfoWithJobInfo(requestInfo)
        return requestInfo

    def getActiveData(self, listStatuses, jobInfoFlag=False):
        return self.getRequestByStatus(listStatuses, jobInfoFlag)

    def getT0ActiveData(self, jobInfoFlag=False):

        return self.getRequestByStatus(T0_ACTIVE_STATUS, jobInfoFlag)

    def getRequestByStatus(self, statusList, jobInfoFlag=False, limit=None, skip=None,
                           legacyFormat=False):

        """
        To use this function reqDBURL need to be set when wmstats initialized.
        This will be deplicated so please don use this.
        If legacyFormat is True convert data to old wmstats format from current reqmgr format.
        Shouldn't be set to True unless existing code breaks
        """
        results = dict()
        for status in statusList:
            self.logger.info("Fetching workflows by status from ReqMgr2, status: %s", status)
            requestInfo = self.reqDB.getRequestByStatus(status, True, limit, skip)
            self.logger.info("Found %d workflows in status: %s", len(requestInfo), status)

            if legacyFormat:
                # convert the format to wmstats old format
                for requestName, doc in viewitems(requestInfo):
                    requestInfo[requestName] = convertToLegacyFormat(doc)
            results.update(requestInfo)

        # now update these requests with agent information too
        if results and jobInfoFlag:
            self.logger.info("Now updating these requests with job info...")
            self._updateRequestInfoWithJobInfo(results)

        return results

    def getRequestSummaryWithJobInfo(self, requestName):
        """
        get request info with job status
        """
        requestInfo = self.reqDB.getRequestByNames(requestName)
        self._updateRequestInfoWithJobInfo(requestInfo)
        return requestInfo

    def getArchivedRequests(self):
        """
        get list of archived workflow in wmstats db.
        """

        options = {"group_level": 1, "reduce": True}

        results = self._getCouchView("allWorkflows", options)['rows']
        requestNames = [x['key'] for x in results]

        workflowDict = self.reqDB.getStatusAndTypeByRequest(requestNames)
        archivedRequests = []
        for request, value in viewitems(workflowDict):
            if value[0].endswith("-archived"):
                archivedRequests.append(request)

        return archivedRequests

    def isWorkflowCompletedWithLogCollectAndCleanUp(self, requestName):
        """
        check whether workflow  is completed including LogCollect and CleanUp tasks
        TODO: If the parent task all failed and next task are not created at all,
            It can't detect complete status.
            If the one of the task doesn't contain any jobs, it will return False
        """

        requestInfo = self.getRequestSummaryWithJobInfo(requestName)
        reqInfoInstance = RequestInfo(requestInfo[requestName])
        return reqInfoInstance.isWorkflowFinished()

    def getTaskJobSummaryByRequest(self, requestName, sampleSize=1, usePycurl=True):
        reqStart = time.time()
        options = {'reduce': True, 'group_level': 5, 'startkey': [requestName],
                   'endkey': [requestName, {}]}
        results = self._getCouchView("jobsByStatusWorkflow", options)

        jobDetails = {}
        if usePycurl is True:
            jobDetails = getTaskJobSummaryByRequestPycurl(results, sampleSize, self.serviceOpts)
        else:
            # then it is sequential
            for row in results['rows']:
                # row["key"] = ['workflow', 'task', 'jobstatus', 'exitCode', 'site']
                startKey = row["key"][:4]
                endKey = []
                site = row["key"][4]
                if site:
                    startKey.append(site)

                endKey.extend(startKey)
                endKey.append({})
                numOfError = row["value"]

                jobInfo = self.jobDetailByTasks(startKey, endKey, numOfError, sampleSize)
                jobDetails = nestedDictUpdate(jobDetails, jobInfo)
        callRuntime = round((time.time() - reqStart), 3)
        msg = f"Retrieved job details (pycurl mode: {usePycurl}) for {requestName} in "
        msg += f"{callRuntime} seconds, with a total of {len(results['rows']) + 1} CouchDB calls"
        print(msg)
        return jobDetails

    def jobDetailByTasks(self, startKey, endKey, numOfError, limit=1):
        options = {'include_docs': True, 'reduce': False,
                   'startkey': startKey, 'endkey': endKey,
                   'limit': limit}
        result = self._getCouchView("jobsByStatusWorkflow", options)
        jobInfoDoc = {}
        for row in result['rows']:
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
            finalStruct["errorCount"] = numOfError
            finalStruct.setdefault("samples", [])
            finalStruct["samples"].append(row["doc"])

        return jobInfoDoc

    def getAllAgentRequestRevByID(self, agentURL):
        options = {"reduce": False}
        results = self._getCouchView("byAgentURL", options, keys=[agentURL])
        idRevMap = {}
        for row in results['rows']:
            idRevMap[row['id']] = row['value']['rev']

        return idRevMap
