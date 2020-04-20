WMStats.namespace("GenericRequests");
WMStats.namespace("GenericRequestsSummary");
WMStats.namespace("RequestsByKey");

WMStats.GenericRequestsSummary = function (summaryStruct) {
    
    this._get = WMStats.Utils.get;
       
    this.summaryStruct = {length: 0};
    //default job status structure
    this.jobStatus = {
             success: 0,
             canceled: 0,
             transition: 0,
             queued: {first: 0, retry: 0},
             submitted: {first: 0, retry: 0},
             submitted: {pending: 0, running: 0},
             failure: {create: 0, submit: 0, exception: 0},
             cooloff: {create: 0, submit: 0, job: 0},
             paused: {create: 0, submit: 0, job: 0},
         };
    

    if (summaryStruct) {
        this.summaryStruct = WMStats.Utils.cloneObj(summaryStruct);
    };
};

WMStats.GenericRequestsSummary.prototype = {
    
    getJobStatus: function(statusStr) {
        return WMStats.Utils.get(this.jobStatus, statusStr, 0);
    },
    
    getAvgProgressSummary: function (doc) {
        
		var progressStat = {};
        var datasets = {};
        var numDataset = 0;
        if (doc.outputdatasets) {
        	numDataset = doc.outputdatasets.length;	
        }
        
        for (var task in doc.tasks) {
            for(var site in doc.tasks[task].sites) {
                for (var outputDS in doc.tasks[task].sites[site].dataset) {
                    if (datasets[outputDS] === undefined) {
                        if (!doc.outputdatasets) {
                        	//if outputdatasets is not defined calcuate from frwj
                        	numDataset += 1;
                        }
                        datasets[outputDS] = true;
                    }
                    WMStats.Utils.updateObj(progressStat, doc.tasks[task].sites[site].dataset[outputDS]);
                }
             }
        }
        for (var prop in progressStat) {
            progressStat[prop] = progressStat[prop] / numDataset;
        }
        progressStat.numDataset = numDataset;
        return progressStat;
    },
    
    getAvgEvents: function() {
        // handle legacy event calculation
        if (this.summaryStruct.progress === undefined || this.summaryStruct.progress.events === undefined) {
            return this.summaryStruct.processedEvents;
        } else {
            return this.summaryStruct.progress.events;
        }
    },
    
    getAvgLumis: function() {
        // handle legacy event calculation
        if (this.summaryStruct.progress.totalLumis === undefined) {
            return 0;
        } else {
            return this.summaryStruct.progress.totalLumis;
        }
    },
    
    getSummary: function(){
        return this.summaryStruct;
    },
    
    summaryStructUpdateFuction: null,
    
    update: function(summary) {
        WMStats.Utils.updateObj(this.summaryStruct, summary.summaryStruct, true, 
                                this.summaryStructUpdateFuction);
        WMStats.Utils.updateObj(this.jobStatus, summary.jobStatus);
    },
    
    updateFromRequestDoc: function(doc) {
         var summary = this.createSummaryFromRequestDoc(doc);
         this.update(summary);
    },
    
    getWMBSTotalJobs: function() {
        return (this.getJobStatus("success") +
                this.getJobStatus("canceled") +
                this.getJobStatus( "transition") +
                this.getTotalFailure() +
                this.getTotalCooloff() +
                this.getTotalPaused() +
                this.getTotalQueued() +
                this.getRunning() +
                this.getPending());
    },
    
    getTotalFailure: function() {
        return (this.getJobStatus("failure.create") + 
                this.getJobStatus("failure.submit") + 
                this.getJobStatus("failure.exception"));
    },
    
    getTotalSubmitted: function() {
        return (this.getJobStatus("submitted.first") + 
                this.getJobStatus("submitted.retry"));
    },

    getRunning: function() {
        return this.getJobStatus("submitted.running");
    },
    
    getPending: function() {
        return this.getJobStatus("submitted.pending");
    },
    getTotalCooloff:function() {
        return (this.getJobStatus("cooloff.create") + 
                this.getJobStatus("cooloff.submit") + 
                this.getJobStatus("cooloff.job"));
    },

    getTotalPaused: function() {
        return (this.getJobStatus("paused.create") + 
                this.getJobStatus("paused.submit") + 
                this.getJobStatus("paused.job"));
    },
    
    getTotalQueued: function() {
        return (this.getJobStatus("queued.first") + 
                this.getJobStatus("queued.retry"));
    },
    
    createSummaryFromRequestDoc: function(doc) {
        //THis is just interface which update summarySturct property
        var summary = WMStats.RequestsSummary();
        summary.summaryStruct.length = 1;
        summary.jobStatus = this._get(doc, 'status', {});
        return summary;
    }
};

WMStats.RequestStruct = function(requestName) {
    this._workflow = requestName;
    this._summary = WMStats.RequestsSummary();
    // number of requests in the data
	this._addJobs = WMStats.Utils.updateObj;
};

WMStats.RequestStruct.prototype = {
    
    getProgressStat: function () {
        var progressStat = {};
        for (var task in this.tasks) {
            for(var site in this.tasks[task].sites) {
                WMStats.Utils.updateObj(progressStat, this.tasks[task].sites[site].dataset);
            }
        }
        return progressStat;
    },

    getName: function() {
        return this._workflow;
    },
    
    getSummary: function() {
        return this._summary.createSummaryFromRequestDoc(this);
    },
    
    getTasks: function() {
    	return new WMStats.Tasks(this.tasks, this._workflow);
    },
    
    getLastState: function() {
        if (this.request_status){
            return this.request_status[this.request_status.length -1].status;
        };
        return null;
    },
    
    getLastStateAndTime: function() {
        if (this.request_status){
            return this.request_status[this.request_status.length -1];
        };
        return null;
    },
    
    getSkippedStatus: function () {
    	if (this.skipped) {
    		return this.skipped;
    	} else {
    		return false;
    	}
    },
    
    getSkippedDetail: function(fullTaskName) {
    	var skippedTasks = {};
    	if (this.skipped) {
    		for (var task in this.tasks) {
    			if (this.tasks[task].skipped) {
    				var taskName = task;
    				if (!fullTaskName){
    					var taskList = task.split('/');
    					taskName = taskList[taskList.length - 1];
    				}
    				skippedTasks[taskName] = this.tasks[task].skipped;
        		}
        	}
        }
        return skippedTasks;
    },
    
    updateFromCouchDoc: function (doc) {
        
        function _tasksUpdateFunction (baseObj, addObj, field) {
            if (field === "JobType") {
                baseObj[field] = addObj[field];
            } else if (field === "updated"){
                baseObj[field] = Math.max(baseObj[field], addObj[field]);
            } else {
                baseObj[field] += addObj[field];
            }
        }; 
        
        for (var field in doc) {
            //handles when request is split in more than one agents
            if (field == "AgentJobInfo") {
            	//skipping AgentJobInfo field. - added to handle newer ajax call to wmstats server
            	continue;
            } else if (this[field] && 
                (field == 'sites' || field == 'status')){
                this._addJobs(this[field], doc[field]);
            } else if (this[field] && field == 'tasks'){
            	//Also task['skipped'] value will be updated here
                this._addJobs(this[field], doc[field], true,  _tasksUpdateFunction);
            
            } else if (this[field] && field == 'output_progress') {
                var outProgress = this.output_progress;
                for (var index in outProgress){
                    for (var prop in doc[field][index]) {
                        outProgress[index][prop] += doc[field][index][prop];
                        //TODO: need combine dataset separately
                    }
                }
            } else if (this[field] && field == 'skipped') {
                this[field] = this[field] || doc[field]; 
            } else if (field == 'agent_url') {
                if (this[field] === undefined) this[field] = [];
                WMStats.Utils.addToSet(this[field], doc[field]);
            
            } else {
                this[field] = doc[field];
            }
        }
    }
};

WMStats.GenericRequests = function (data) {
    /*
     * Data structure for holding the request
     * it handles 3 types (not very robust and modular)
     * TODO: remove dependencies from different data type. (tier0, reqmgr)
     * if possible
     * reqmgr_request, agent_request, tier0_request.
     */
    // request data by workflow name
    this._dataByWorkflow = {};
    // request data by agent - only contains information from agent
    // i.e. job status.
    this._dataByWorkflowAgent = {};
    this._get = WMStats.Utils.get;
    this._filter = {};
    this._filteredRequests = null;
    if (data !== undefined) {
    	this.setFromRawData(data);
    }
};

WMStats.GenericRequests.prototype = {

    _mapProperty: function (workflowData, property) {
        if (property == 'request_status') {
            return workflowData[property][workflowData[property].length - 1].status;
        }
        if (property == 'inputdataset') {
            return WMStats.Utils.getInputDatasets(workflowData);
        }
        return workflowData[property];
    },
    
    _getRequestObj: function (request) {
        if (typeof(request) == "string") {
            return this.getData(request);
        } else {
            return request;
        }
    },
    
    _getStatusObj: function(request, level) {
        //level could be site, task, or request;
        var requestObj = this._getRequestObj(request);
        if (level == "task"){
            return requestObj.tasks.status;
        } else if (level == "site") {
            return requestObj.sites;
        } else {
            return requestObj.status;
        }
    },

    _requestDateSort: function(a, b) {
        for (var i in a.request_date) { 
            if (b.request_date[i] != a.request_date[i]) {
                return (Number(b.request_date[i]) - Number(a.request_date[i]));
            }
        }
        return 0;
    },

   _andFilter: function(base, filter) {
        var includeFlag = true;
        for (var property in filter) {
            if (!filter[property]) {
                continue;
            }else if (this._mapProperty(base, property) !== undefined &&
               this._contains(this._mapProperty(base, property), filter[property])) {
                continue;
            } else {
                includeFlag = false;
                break;
            }
        }
        return includeFlag;
    },
    
    _contains: function(a, b) {
        //TODO change to regular expression or handle numbers
        if ((typeof a) === "string") return (a.toLowerCase().indexOf(b.toLowerCase()) !== -1);
        else if ((typeof a) == "number") return (Number(b) == a);
        else if (a instanceof Array) {
            for (var i in a) {
                if (this._contains(a[i], b)) return true;
            }
            return false;
        } else {
            alert("value need to be either number or string");
        }
    },
        
    getProgressStat: function (request) {
        var requestObj = this._getRequestObj(request);
        return requestObj.getProgressStat();
    },
    
    getFilter: function() {
        return this._filter;
    },
    
    setFilter: function(filter) {
        this._filter = filter;
    },
    
    updateRequest: function(doc) {
        /*
         * 
         */
        var doc = WMStats.Globals.convertRequestDocToWMStatsFormat(doc);
        var workflow = doc.workflow;
        var agentURL = doc.agent_url;
        
        if (workflow && !this._dataByWorkflow[workflow]) {
            this._dataByWorkflow[workflow] = new WMStats.RequestStruct(workflow);;
        }
        
        if (agentURL && !this._dataByWorkflowAgent[workflow]) {
            this._dataByWorkflowAgent[workflow] = {};
        }
        //if it is new agent create one.
        if (agentURL && !this._dataByWorkflowAgent[workflow][agentURL]){
            this._dataByWorkflowAgent[workflow][agentURL] = new WMStats.RequestStruct(workflow);
        }
        
        // update both _databyWorkflow
        this.getData(workflow).updateFromCouchDoc(doc);
        if (agentURL) {
            this.getData(workflow, agentURL).updateFromCouchDoc(doc);
        }
    },
    
    updateBulkRequests: function(docList) {
        for (var row in docList) {
            //not sure why there is null case
            if (docList[row].doc) {
                this.updateRequest(docList[row].doc);
            }
        }
    },
    
    updateRequestFromWMStatsServer: function(doc) {
    	
    	var doc = WMStats.Globals.convertRequestDocToWMStatsFormat(doc);
        var workflow = doc.workflow;
        
        if (workflow && !this._dataByWorkflow[workflow]) {
            this._dataByWorkflow[workflow] = new WMStats.RequestStruct(workflow);
            this._dataByWorkflow[workflow].updateFromCouchDoc(doc);
        };
        
        if (doc.AgentJobInfo) {
        	
        	for (var agentURL in doc.AgentJobInfo) {
        		if (agentURL && !this._dataByWorkflowAgent[workflow]) {
            		this._dataByWorkflowAgent[workflow] = {};
        		};
        		this._dataByWorkflowAgent[workflow][agentURL] = new WMStats.RequestStruct(workflow);
        		this._dataByWorkflowAgent[workflow][agentURL].updateFromCouchDoc(doc.AgentJobInfo[agentURL]);
        		// legacy format which need to be updated
        		doc.AgentJobInfo[agentURL].agent_url = agentURL;
        		this._dataByWorkflow[workflow].updateFromCouchDoc(doc.AgentJobInfo[agentURL]);
        	};
        };

    },
    
    setFromRawData: function(data) {
    	/*   {"result": [
                                 * 	{"sryu_ReReco_reqmgr2_validation_150717_180724_9878": 
                                 *   {"InputDataset": "/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN", 
                                 *    "Group": "DATAOPS", "CustodialSites": [], "OpenRunningTimeout": 1800, 
                                 *    "Comments": "MCFromGEN LumiBased splitting with 1l per job. Half an hour opened", 
                                 *    "Requestor": "sryu", "ProcessingString": "START53_V7C", "ScramArch": "slc6_amd64_gcc472", 
                                 *    "SizePerEvent": 1154, "ConfigCacheID": "1ad063a0d73c1d81143b4182cbf84793", "Memory": 2300, 
                                 *    "RunBlacklist": [], "PrepID": "B2G-Summer12-00736", "AutoApproveSubscriptionSites": [], 
                                 *    "BlockBlacklist": [], "BlockWhitelist": [], "CustodialSubType": "Move", 
                                 *    "RequestType": "ReReco", "TimePerEvent": 16.87
                                 *    "OutputDatasets": ["/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Integ_Test-ReReco_SRYU_pnn-v1/GEN-SIM"], 
                                 *    "LumisPerJob": 1, "SoftwareVersions": ["CMSSW_5_3_19"], 
                                 *    "AcquisitionEra": "Integ_Test", "PrimaryDataset": "QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph", 
                                 *    "CouchDBName": "reqmgr_config_cache", "CMSSWVersion": "CMSSW_5_3_19", "NonCustodialSites": [], 
                                 *    "RequestSizeFiles": 0, "CouchWorkloadDBName": "reqmgr_workload_cache", "RequestPriority": 90000, 
                                 *    "SiteWhitelist": ["T1_US_FNAL", "T2_CH_CERN"], 
                                 *    "SubscriptionPriority": "Low", "ProcessingVersion": "1", "Team": "testbed-dev",
                                 *    "SplittingAlgo": "LumiBased", "TotalEstimatedJobs": 100, 
                                 *    "RequestTransition": [{"Status": "new", "DN": null, "UpdateTime": 1437149248}, 
                                 *                          {"Status": "assignment-approved", "DN": null, "UpdateTime": 1437149248}, 
                                 *                          {"Status": "assigned", "DN": null, "UpdateTime": 1437149249}, 
                                 *                          {"Status": "acquired", "DN": null, "UpdateTime": 1437150308}, 
                                 *                          {"Status": "running-open", "DN": null, "UpdateTime": 1437152703}, 
                                 *                          {"Status": "running-closed", "DN": null, "UpdateTime": 1437152709}, 
                                 *                          {"Status": "completed", "DN": null, "UpdateTime": 1437227104}], 
                                 *    "RequestName": "sryu_ReReco_reqmgr2_validation_150717_180724_9878", 
                                 *    "RequestString": "ReReco_reqmgr2_validation", 
                                 *    "InputDatasets": ["/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN"], 
                                 *    "CouchURL": "https://reqmgr2-dev.cern.ch/couchdb", "TotalTime": 28800, 
                                 *    "RequestorDN": "....", 
                                 *    "RequestWorkflow": "https://reqmgr2-dev.cern.ch/couchdb/reqmgr_workload_cache/sryu_ReReco_reqmgr2_validation_150717_180724_9878/spec", "
                                 *    "Campaign": "Agent108_Validation", "GlobalTag": "START53_V7C::All", "RunWhitelist": [], 
                                 *    "FilterEfficiency": 1, "DbsUrl": "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader", 
                                 *    "TotalInputLumis": 100, "RequestDate": [2015, 7, 17, 16, 7, 24], "NonCustodialSubType": "Replica", 
                                 *    "TotalInputFiles": 1, "SiteBlacklist": [], "TotalInputEvents": 2500, 
                                 *    "ConfigCacheUrl": "https://cmsweb.cern.ch/couchdb", 
                                 *    "_id": "sryu_ReReco_reqmgr2_validation_150717_180724_9878", 
                                 *    "RequestStatus": "completed", 
                                 *    "RequestNumEvents": 50000, 
                                 *    "AgentJobInfo": {"vocms008.cern.ch:9999": 
                                 *                       {"status": {"success": 107}, 
                                 *                        "agent_team": "testbed-dev", 
                                 *                        "workflow": "sryu_ReReco_reqmgr2_validation_150717_180724_9878", 
                                 *                        "timestamp": 1437498506, 
                                 *                        "_rev": "1-d792c9d73285ff1318d7f5c0e2b2f486", 
                                 *                        "sites": {"T2_CH_CERN": {"success": 107}}, 
                                 *                        "agent": "WMAgent", 
                                 *                        "tasks": {"/sryu_ReReco_reqmgr2_validation_150717_180724_9878/ReReco/ReRecoMergeRAWSIMoutput/ReRecoRAWSIMoutputMergeLogCollect": 
                                 *                                     {"status": {"success": 1}, 
                                 *                                      "sites": {"T2_CH_CERN": 
                                 *                                                  {"inputEvents": 0, 
                                 *                                                   "cmsRunCPUPerformance": {"totalJobCPU": 0, "totalJobTime": 0, "totalEventCPU": 0}, 
                                 *                                                   "wrappedTotalJobTime": 6, "success": 1, "dataset": {}}}}, 
                                 *                                   "/sryu_ReReco_reqmgr2_validation_150717_180724_9878/ReReco/LogCollect": 
                                 *                                      {"status": {"success": 1}, 
                                 *                                       "sites": .....}}}, ....}, 
                                 *                        "agent_url": "vocms008.cern.ch:9999", 
                                 *                        "_id": "98e5e3643f1c6575407b6de04bd6619a", 
                                 *                        "type": "agent_request"}},
                                 * "sryu_ReReco_reqmgr2_validation_150721_194418_5448": 
                                 *   {"InputDataset": .....
                                 *   }}]
                                 * }
                                 */
    	if (data.result.length == 1) {
    		for (var req in data.result[0]) {
    			this.updateRequestFromWMStatsServer(data.result[0][req]);
    		}
    	}
    },

    filterRequests: function(filter) {
        var requestData = this.getData();
        var filteredData = {};
        var requestWithAgentData = this.getDataWithAgent();
        var filteredDataWithAgent = {};
        if (filter === undefined) {filter = this._filter;}
        for (var workflowName in requestData) {
            if (this._andFilter(requestData[workflowName], filter)){
                filteredData[workflowName] =  requestData[workflowName];
                filteredDataWithAgent[workflowName] =  requestWithAgentData[workflowName];
            }
        }
        this._filteredRequests = WMStats.Requests();
        this._filteredRequests.setDataByWorkflow(filteredData, filteredDataWithAgent);
        return this._filteredRequests;
    },

    getKeyValue: function(request, keyString, defaultVal) {
        //keyString is opject property separte by '.'
        return this._get(this._dataByWorkflow[request], keyString, defaultVal);
    },
    
    getData: function(workflow, agentURL) {
        if (workflow && (agentURL === "all" || agentURL === "NA" )) {
            return this._dataByWorkflowAgent[workflow];
        } else if (workflow && agentURL) {
            return this._dataByWorkflowAgent[workflow][agentURL];
        } else if (workflow){
            return this._dataByWorkflow[workflow];
        } else{
            return this._dataByWorkflow;
        }
    },
    
    getDataWithAgent: function(workflow, agentURL) {
        if (workflow && (agentURL === "all" || agentURL === "NA" )) {
            return this._dataByWorkflowAgent[workflow];
        } else if (workflow && agentURL) {
            return this._dataByWorkflowAgent[workflow][agentURL];
        } else{
            return this._dataByWorkflowAgent;
        }
    },
    
    getFilteredRequests: function() {
        return this._filteredRequests;
    },
    
    setDataByWorkflow: function(data, agentData) {
        //keyString is opject property separte by '.'
        this._dataByWorkflow = data;
        this._dataByWorkflowAgent = agentData;
    },
    
    getList: function(sortFunc) {
        var list = [];
        for (var request in this.getData()) {
            list.push(this.getData(request));
        }
        if (sortFunc) {
            return list.sort(sortFunc);
        } else {
            return list.sort(this._requestDateSort);
        }
    },
    
    getRequestNames: function() {
        var list = [];
        for (var request in this.getData()) {
            list.push(request);
        }
        return list;
    },

    getSummary: function(workflow, agentURL) {
        
        var requests = this.getData(workflow, agentURL);
        if (workflow) {
            return requests.getSummary();
        } else {
            var summary =  WMStats.RequestsSummary();
            //TODO need to cache the information
            for (var requestName in requests) {
                summary.update(this.getData(requestName).getSummary());
            }
            return summary;
        }
    },
    
    getAlertRequests: function() {
        var alertRequests = [];
        for (var workflow in this.getData()) {
            var reqSummary = this.getSummary(workflow);
            var coolOff = reqSummary.getTotalCooloff();
            var paused = reqSummary.getTotalPaused();
            if (coolOff > 0 || paused > 0) {
                alertRequests.push(this.getData(workflow));
            }
        }
        return alertRequests;
    },
    
    getRequestStatusAndTime: function(workflowName) {
        var workflowData = this._dataByWorkflow[workflowName];
        return  workflowData["request_status"][workflowData["request_status"].length - 1];
    },
    
    getTasks: function(workflowName) {
    	return this._dataByWorkflow[workflowName].getTasks();
    }
    
};

WMStats.RequestsByKey = function (category, summaryFunc) {
    
    var _data = {};
    var _category = category;
    var _get = WMStats.Utils.get;
    
    function categorize(requestData) {
        
        function _getRequestData(workflow, agentURL){
            if (_category === "agent" && agentURL !== "all" && agentURL !== "NA" ) {
                return requestData.getData(workflow, agentURL);
            } else {
                return requestData.getData(workflow);
            }
        }
        
        function _getCategoryKey(workflow){
            if (_category === "agent") {
                var agentCategory = requestData.getData(workflow, "all");
                if (agentCategory === undefined) {
                    return "NA";
                } else {
                    return agentCategory;
                }
            } else {
                return requestData.getKeyValue(workflow, _category, "NA");
            }
        }
        function _updateData(key, workflow, summaryBase) {
            if (_data[key] === undefined) {
                //initial set up
                _data[key] = {};
                _data[key].requests = {};
                _data[key].summary =  summaryFunc();
                _data[key].key = key;
            }
            var requestInfo = _getRequestData(workflow, key);
            _data[key].requests[workflow] = requestInfo;
            _data[key].summary.updateFromRequestDoc(summaryBase);
        };
        
        var dataByWorkflow = requestData.getData();
        for (var workflow in dataByWorkflow) {
            var key = _getCategoryKey(workflow);
            if (typeof key == 'object') {
                if (key.length) {
                    // handles array case
                    for (var index in key) {
                        _updateData(key[index], workflow, requestData.getData(workflow));
                    }
                } else {
                    // handles agent, sites and tasks case
                    for (var prop in key) {
                        _updateData(prop, workflow, key[prop]);
                    }
                }
                
            } else {
                if (key == "NA" && _category == "sites" || _category == "tasks" || _category == "agent") {
                    // summary base shouldn't be higher level. since sites and tasks
                    // has sub hierarchy
                    _updateData(key, workflow, {});
                } else {
                    _updateData(key, workflow, requestData.getData(workflow));
                }
            }
            
        }
    };
    
    function getData(key){
        if (key === undefined) {
            return _data;
        } else {
            return _data[key];
        }
    };
    
    function getRequestData(key){
        var requestData = WMStats.Requests();
        if (_data[key] !== undefined) {
            requestData.setDataByWorkflow(_data[key].requests);
        }
        return requestData;
    };
    
    function getList(sortFunc) {
        var list = [];
        for (var key in _data) {
            list.push(_data[key]);
        }
        if (sortFunc) {
            return list.sort(sortFunc);
        } else {
            return list;
        }
    };
    
    return {
        categorize: categorize,
        getData: getData,
        getRequestData: getRequestData,
        category: _category,
        getList: getList
    };
};
