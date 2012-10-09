WMStats.namespace("GenericRequests");
WMStats.namespace("GenericRequestsSummary");
WMStats.namespace("RequestsByKey");

WMStats.GenericRequestsSummary = function (summaryStruct) {
    
    this._get = WMStats.Utils.get;
    // number of requests in the data
    this._addJobs = WMStats.Utils.updateObj;
    
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
        for (var prop in summaryStruct) {
            // only works with one level deep summary
            this.summaryStruct[prop] = summaryStruct[prop];
        };
    };
};

WMStats.GenericRequestsSummary.prototype = {
    
    getJobStatus: function(statusStr) {
        return WMStats.Utils.get(this.jobStatus, statusStr, 0);
    },

    getSummary: function(){
        return this.summaryStruct;
    },
    
    update: function(summary) {
        WMStats.Utils.updateObj(this.summaryStruct, summary.summaryStruct);
        WMStats.Utils.updateObj(this.jobStatus, summary.jobStatus)
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
                this.getTotalSubmitted());
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
        summary.jobStatus = this._get(doc, 'status', {})
        return summary;
    }
};

WMStats.GenericRequests = function (noFilterFlag) {
    /*
     * Data structure for holding the request
     * it handles 3 types (not very robust and modular)
     * TODO: remove dependencies from different data type. (tier0, analysis, reqmgr)
     * if possible
     * reqmgr_request, agent_request, tier0_request.
     */
    // request data by workflow name
    this._dataByWorkflow = {};
    this._dataByWorkflowAgent = {}
    this._get = WMStats.Utils.get;
    // number of requests in the data
    this._addJobs = WMStats.Utils.updateObj;
    this._filter = {};
    this._filteredRequests = noFilterFlag || WMStats.Requests(true);
}

WMStats.GenericRequests.prototype = {

    _mapProperty: function (workflowData, property) {
        if (property == 'request_status') {
            return workflowData[property][workflowData[property].length - 1].status;
        } 
        return workflowData[property];
    },
    
    _getRequestObj: function (request) {
        if (typeof(request) == "string") {
            return this.getDataByWorkflow(request);
        } else {
            return request;
        }
    },
    
    _getStatusObj: function(request, level) {
        //level could be site, task, or request;
        var requestObj = this._getRequestObj(request)
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
            if (this._mapProperty(base, property) !== undefined &&
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
        //TODO change to regular expression
        return (!b || a.toLowerCase().indexOf(b.toLowerCase()) !== -1);
    },
    
    getFilter: function() {
        return this._filter;
    },
    
    setFilter: function(filter) {
        this._filter = filter;
    },
    
    updateRequest: function(doc) {

        var request = this.getDataByWorkflow(doc.workflow);
        if (!request) {
            this._dataByWorkflow[doc.workflow] = {};
        }
        
        var requestWithAgent = this.getRequestByNameAndAgent(doc.workflow, doc.agent_url);
         
        for (var field in doc) {
            //handles when request is splited in more than one agents
            if (this._dataByWorkflow[doc.workflow][field] && 
                (field == 'sites' || field == 'status')){
                this._addJobs(this._dataByWorkflow[doc.workflow][field], doc[field])
            } else if (this._dataByWorkflow[doc.workflow][field] && field == 'output_progress') {
                var outProgress = this._dataByWorkflow[doc.workflow].output_progress;
                for (var index in outProgress){
                    for (var prop in doc[field][index]) {
                        outProgress[index][prop] += doc[field][index][prop];
                        //TODO: need combine dataset separtely
                    }
                }
            } else {
                this._dataByWorkflow[doc.workflow][field] = doc[field];
            }
            //for request, agenturl structure
            requestWithAgent[field] = doc[field];
        }
    },
    
    updateBulkRequests: function(docList) {
        for (var row in docList) {
            this.updateRequest(docList[row].doc);
        }
    },

    filterRequests: function() {
        var requestData = this.getDataByWorkflow();
        var filteredData = {}
        for (var workflowName in requestData) {
            if (this._andFilter(requestData[workflowName], this._filter)){
                filteredData[workflowName] =  requestData[workflowName];
            }
        }
        this._filteredRequests.setDataByWorkflow(filteredData);
        return this._filteredRequests;
    },
    

    getRequestByNameAndAgent: function(workflow, agentUrl) {
        if (!this._dataByWorkflowAgent[workflow]) {
                this._dataByWorkflowAgent[workflow] = {}
        }
        
        if (!agentUrl){
            return this._dataByWorkflowAgent[workflow];
        } else {
            if (!this._dataByWorkflowAgent[workflow][agentUrl]){
                this._dataByWorkflowAgent[workflow][agentUrl] = {};
            }
            return this._dataByWorkflowAgent[workflow][agentUrl];
        }
    },
    
    getDataByWorkflow: function(request, keyString, defaultVal) {
        "keyString is opject property separte by ."
        if (!request) return this._dataByWorkflow;
        else if (!keyString) return this._dataByWorkflow[request];
        else return this._get(this._dataByWorkflow[request], keyString, defaultVal);
    },
    
    getData: function(workflow) {
        if (workflow){
            var requestsObj = {};
            requestsObj[workflow] = this._dataByWorkflow[workflow]
            return {requests: requestsObj,
                    summary: this.getSummary(workflow),
                    key: workflow}
        }
        return this._dataByWorkflow;
    },
    
    getFilteredRequests: function() {
        return this._filteredRequests;
    },
    
    setDataByWorkflow: function(data) {
        "keyString is opject property separte by ."
        this._dataByWorkflow = data;
    },
    
    getList: function(sortFunc) {
        var list = [];
        for (var request in this.getDataByWorkflow()) {
            list.push(this.getDataByWorkflow(request))
        }
        if (sortFunc) {
            return list.sort(sortFunc);
        } else {
            return list.sort(this._requestDateSort);
        }
    },

    getSummary: function(workflow) {
        var summary =  WMStats.RequestsSummary();
        
        if (workflow) {
            return summary.createSummaryFromRequestDoc(this.getDataByWorkflow(workflow));
        } else {
            //TODO need to cache the information
            for (var request in this.getDataByWorkflow()) {
                summary.updateFromRequestDoc(this.getDataByWorkflow(request));
            }
            return summary;
        }
    },
    
    getAlertRequests: function() {
        var alertRequests = [];
        for (var workflow in this.getDataByWorkflow()) {
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
        var workflowData = this._dataByWorkflow[workflowName]
        return  workflowData["request_status"][workflowData["request_status"].length - 1];
    }
};

WMStats.RequestsByKey = function (category, summaryFunc) {
    
    var _data = {};
    var _category = category;
    var _get = WMStats.Utils.get;
    
    function categorize(requestData) {
        
        function _updateData(key, summaryBase) {
            if (_data[key] === undefined) {
                //initial set up
                _data[key] = {};
                _data[key].requests = {};
                _data[key].summary =  summaryFunc();
                _data[key].key = key;
            }
            _data[key].requests[workflow] = dataByWorkflow[workflow];
            _data[key].summary.updateFromRequestDoc(summaryBase)
        };

        var dataByWorkflow = requestData.getData();
        for (var workflow in dataByWorkflow) {
            var key = _get(dataByWorkflow[workflow], _category, "NA");
            if (typeof key == 'object') {
                // handles sites and tasks case
                for (var prop in key) {
                    _updateData(prop, key[prop]);
                }
            } else {
                if (key == "NA" && _category == "sites" || _category == "tasks") {
                    // summary base shouldn't be higher level. since sites and tasks
                    // has sub hierierky
                    _updateData(key, {});
                } else {
                    _updateData(key, dataByWorkflow[workflow]);
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
    
    function getList(sortFunc) {
        var list = [];
        for (var key in _data) {
            list.push(_data[key])
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
        category: _category,
        getList: getList
    }
};
