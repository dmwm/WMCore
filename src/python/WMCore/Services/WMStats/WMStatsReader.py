import time
import logging
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL
from WMCore.Wrappers.JsonWrapper import JSONEncoder

class WMStatsReader():
    
    #TODO need to get this from reqmgr api
    ACTIVE_STATUS = ["new",
                    "assignment-approved",
                    "assigned",
                    "ops-hold",
                    "negotiating",
                    "acquired",
                    "running",
                    "running-open",
                    "running-closed",
                    "failed",
                    "completed",
                    "closed-out",
                    "announced",
                    "aborted",
                    "rejected"]

    def __init__(self, couchURL, dbName = None):
        couchURL = sanitizeURL(couchURL)['url']
        # set the connection for local couchDB call
        self._commonInit(couchURL, dbName)
        
    def _commonInit(self, couchURL, dbName):
        """
        setting up comon variables for inherited class.
        inherited class should call this in their init function
        """
        if dbName:
            self.couchURL = couchURL
            self.dbName = dbName
        else:
            self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchServer = CouchServer(self.couchURL)
        self.couchDB = self.couchServer.connectDatabase(self.dbName, False)
        self.couchapp = "WMStats"
        self.defaultStale = {"stale": "update_after"}
        
    
    def setDefaultStaleOptions(self, options):
        if not options:
            options = {}  
        if not options.has_key('stale'):
            options.update(self.defaultStale)
        return options
            
    def _updateReuestInfoWithJobInfo(self, requestInfo):
        if len(requestInfo.keys()) != 0:
            requestAndAgentKey = self._getRequestAndAgent(requestInfo.keys())
            jobDocIds = self._getLatestJobInfo(requestAndAgentKey)
            jobInfoByRequestAndAgent = self._getAllDocsByIDs(jobDocIds)
            self._combineRequestAndJobData(requestInfo, jobInfoByRequestAndAgent)
            
    def _getCouchView(self, view, options, keys = []):
        
        options = self.setDefaultStaleOptions(options)
            
        if keys and type(keys) == str:
            keys = [keys]
        return self.couchDB.loadView(self.couchapp, view, options, keys)
            
        
    def _formatCouchData(self, data, key = "id"):
        result = {}
        for row in data['rows']:
            if row.has_key('error'):
                continue
            if row.has_key("doc"):
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
        "agent":"WMAgentCommissioning",
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
        for row in jobData["rows"]:
            # condition checks if documents are deleted between calls.
            # just ignore in that case
            if row["doc"]:
                jobInfo = requestData[row["doc"]["workflow"]]
                jobInfo.setdefault("AgentJobInfo", {}) 
                jobInfo["AgentJobInfo"][row["doc"]["agent_url"]] = row["doc"]
    
            
    def _getRequestByNames(self, requestNames, detail = True):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        result = self.couchDB.allDocs(options, requestNames)
        return result
        
    def _getRequestByStatus(self, statusList, detail = True, limit = None, skip = None):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        if limit != None:
            options["limit"] = limit
        if limit != None:
            options["skip"] = skip
        keys = statusList or WMStatsReader.ACTIVE_STATUS
        return self._getCouchView("requestByStatus", options, keys)
    
    def _getRequestAndAgent(self, filterRequest = None):
        """
        returns the [['request_name', 'agent_url'], ....]
        """
        options = {}
        options["reduce"] = True
        options["group"] = True
        result = self._getCouchView("requestAgentUrl", options)
        
        if filterRequest == None:
            keys = [row['key'] for row in result["rows"]]
        else:
            keys = [row['key'] for row in result["rows"] if row['key'][0] in filterRequest]
        return keys
    
    def _getLatestJobInfo(self, keys):
        """
        keys is [['request_name', 'agent_url'], ....]
        returns ids
        """
        options = {}
        options["reduce"] = True
        options["group"] = True
        result = self._getCouchView("latestRequest", options, keys)
        ids = [row['value']['id'] for row in result["rows"]]
        return ids
    
    def _getAllDocsByIDs(self, ids, include_docs = True):
        """
        keys is [id, ....]
        returns document
        """
        if len(ids) == 0:
            return None
        options = {}
        options["include_docs"] =  include_docs
        result = self.couchDB.allDocs(options, ids)
        
        return result

    def _getAgentInfo(self):
        """
        returns all the agents status on wmstats
        """
        options = {}
        result = self._getCouchView("agentInfo", options)
        
        return result
    
    def agentsByTeam(self, ignoreDrain = True):
        """
        return a dictionary like {team:#agents,...}
        """
        result = self._getAgentInfo()
        response = dict()
        for agentInfo in result["rows"]:
            
            teams = agentInfo['value']['agent_team'].split(',')
            for team in teams:
                if team not in response.keys():
                    response[team] = 0
            if ignoreDrain:
                if not agentInfo['value']['drain_mode']:
                    for team in teams:
                        response[team] += 1
            else:
                for team in teams:
                    response[team] += 1
        return response
    
    def workflowsByStatus(self, statusList, format = "list"):
        """
        just return the workflow name for the given status
        need to be depricated
        """
        result = self._getRequestByStatus(statusList, detail = False)

        if format == "dict":
            workflowDict = {}
            for item in result["rows"]:
                workflowDict[item["id"]] = None
            return workflowDict
        else:
            workflowList = []
            for item in result["rows"]:
                workflowList.append(item["id"])
            return workflowList
    
    def getDBInstance(self):
        return self.couchDB
    
    def getHeartbeat(self):
        try:
            return self.couchDB.info();
        except Exception, ex:
            return {'error_message': str(ex)}
    
    def getRequestByNames(self, requestNames, jobInfoFlag = False):
        data = self._getRequestByNames(requestNames, True)

        requestInfo = self._formatCouchData(data)
        if jobInfoFlag:
            # get request and agent info
            self._updateReuestInfoWithJobInfo(requestInfo)
        return requestInfo
    
    def getActiveData(self, jobInfoFlag = False):
        
        return self.getRequestByStatus(WMStatsReader.ACTIVE_STATUS, jobInfoFlag)
    
    def getRequestByStatus(self, statusList, jobInfoFlag = False, limit = None, skip = None):
        
        data = self._getRequestByStatus(statusList, True, limit, skip)
        requestInfo = self._formatCouchData(data)

        if jobInfoFlag:
            # get request and agent info
            self._updateReuestInfoWithJobInfo(requestInfo)
        return requestInfo
    