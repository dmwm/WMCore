WMStats.namespace("Requests");
WMStats.Requests = function (noFilterFlag) {
    /*
     * Data structure for holding the request
     */
    // request data by workflow name
    var _dataByWorkflow = {};
    var _dataByWorkflowAgent = {}
    var _get = WMStats.Utils.get;
    // number of requests in the data
    var _length = 0;
    var _filter = {};
    var _filteredLength = 0;
    var _filteredRequests = noFilterFlag || WMStats.Requests(true);
    
    var _defaultSummary= function() {
        return  {length: 0,
                 totalJobs: 0,
                 totalEvents: 0,
                 processedEvents: 0,
                 success: 0,
                 pending: 0,
                 running: 0,
                 failure: 0,
                 queued: 0};
    };
    
    var _summary = _defaultSummary()
    var _filteredSummary = _defaultSummary()
    
    var statusOrder = {
        "new": 1,
        "testing-approved": 2,
        "testing": 3,
        "tested": 4,
        "test-failed": 5,
        "assignment-approved": 6,
        "assigned": 7,
        "ops-hold": 8,
        "negotiating": 9,
        "acquired": 10,
        "running": 11,
        "failed": 12,
        "epic-FAILED": 13,
        "completed": 14,
        "closed-out": 15,
        "announced": 16,
        "aborted": 17,
        "rejected": 18
    }
    function updateSummary(request, summary) {
        var aData = getDataByWorkflow(request);
        summary.length += 1;
        summary.totalJobs += getWMBSJobsTotal(request);
        summary.totalEvents += Number(_get(aData, "input_events", 0));
        summary.processedEvents += _get(aData, "output_progress.0.events", 0)
        summary.failure += failureTotal(request);
        summary.queued += queuedTotal(request);
        summary.success += _get(aData, "status.success", 0);
        summary.running += _get(aData, "status.submitted.running", 0);
        summary.pending += _get(aData, "status.submitted.pending", 0);
    }
    
    function updateRequestSummary(doc) {
        var request = doc.workflow;
        var aData = getDataByWorkflow(request);
        
        if (doc.type == "agent_request") {
            _summary.totalJobs += getWMBSJobsTotal(doc);
            _summary.processedEvents += _get(doc, "output_progress.0.events", 0)
            _summary.failure += failureTotal(doc);
            _summary.queued += queuedTotal(doc);
            _summary.success += _get(doc, "status.success", 0);
            _summary.running += _get(doc, "status.submitted.running", 0);
            _summary.pending += _get(doc, "status.submitted.pending", 0);
        } else if (doc.type == "reqmgr_request") {
            _summary.totalEvents += Number(_get(doc, "input_events", 0));
            _summary.length++;
        }
    }
    
    function _addJobs(baseObj, additionObj) {
       for (var field in additionObj) {
            if (!baseObj[field]) {
                baseObj[field] = additionObj[field];
            } else {
                if (typeof(baseObj[field]) == "object"){
                    _addJobs(baseObj[field], additionObj[field]);
                } else { // should be number
                    baseObj[field] += additionObj[field];
                }
            }
        } 
    }
    
    function mapProperty(workflowData, property) {
        if (property == 'request_status') {
            return workflowData[property][workflowData[property].length - 1].status;
        } 
        return workflowData[property];
    }
    
    function getLength() {
        return _length;
    }
    
    function getFilter() {
        return _filter;
    }
    
    function setFilter(filter) {
        _filter = filter;
    }
    
    function updateRequest(doc) {

        var request = getDataByWorkflow(doc.workflow);
        if (!request) {
            _length++;
            _dataByWorkflow[doc.workflow] = {};
        }
        
        updateRequestSummary(doc);
        
        var requestWithAgent = getRequestByNameAndAgent(doc.workflow, doc.agent_url);
         
        for (var field in doc) {
            //handles when request is splited in more than one agents
            if (_dataByWorkflow[doc.workflow][field] && 
                (field == 'sites' || field == 'status')){
                _addJobs(_dataByWorkflow[doc.workflow][field], doc[field])
            } else if (_dataByWorkflow[doc.workflow][field] && field == 'output_progress') {
                var outProgress = _dataByWorkflow[doc.workflow].output_progress;
                for (var index in outProgress){
                    for (var prop in doc[field][index]) {
                        outProgress[index][prop] += doc[field][index][prop];
                        //TODO: need combine dataset separtely
                    }
                }
            } else {
                _dataByWorkflow[doc.workflow][field] = doc[field];
            }
            //for request, agenturl structure
            requestWithAgent[field] = doc[field];
        }
    };
    
    function updateBulkRequests(docList) {
        for (var row in docList) {
            updateRequest(docList[row].doc);
        }
    };

    function filterRequests() {
        var requestData = getDataByWorkflow();
        var filteredData = {}
        _filteredSummary = _defaultSummary();
        for (var workflowName in requestData) {
            if (andFilter(requestData[workflowName], _filter)){
                filteredData[workflowName] =  requestData[workflowName];
                updateSummary(workflowName, _filteredSummary);
            }
        }
        _filteredRequests.setDataByWorkflow(filteredData);
        return _filteredRequests;
    }
    
    function andFilter(base, filter) {
        var includeFlag = true;
        for (var property in filter) {
            if (mapProperty(base, property) !== undefined && contains(mapProperty(base, property), filter[property])) {
                continue;
            } else {
                includeFlag = false;
                break;
            }
        }
        return includeFlag;
    }
    
    function contains(a, b) {
        //TODO change to regular expression
        return (!b || a.toLowerCase().indexOf(b.toLowerCase()) !== -1);
    }
    
    function getRequestByNameAndAgent(workflow, agentUrl) {
        if (!_dataByWorkflowAgent[workflow]) {
                _dataByWorkflowAgent[workflow] = {}
        }
        
        if (!agentUrl){
            return _dataByWorkflowAgent[workflow];
        } else {
            if (!_dataByWorkflowAgent[workflow][agentUrl]){
                _dataByWorkflowAgent[workflow][agentUrl] = {};
            }
            return _dataByWorkflowAgent[workflow][agentUrl];
        }
    };
    
    function getDataByWorkflow(request, keyString, defaultVal) {
        "keyString is opject property separte by ."
        if (!request) return _dataByWorkflow;
        else if (!keyString) return _dataByWorkflow[request];
        else return _get(_dataByWorkflow[request], keyString, defaultVal);
    }
    
    function setDataByWorkflow(data) {
        "keyString is opject property separte by ."
        _dataByWorkflow = data;
    }
    
    function getList() {
        var list = [];
        for (var request in getDataByWorkflow()) {
            list.push(getDataByWorkflow(request))
        }
        return list.sort(requestDateSort);
    }
    
    function requestDateSort(a, b) {
        for (var i in a.request_date) { 
            if (b.request_date[i] != a.request_date[i]) {
                return (Number(b.request_date[i]) - Number(a.request_date[i]));
            }
        }
        return 0;
    }
    
    function getWMBSJobsTotal(request) {
        if (typeof(request) == "string") {
            var aData = getDataByWorkflow(request);
        } else {
            var aData = request;
        }
        
        return (_get(aData, "status.success", 0) + 
                _get(aData, "status.cooloff", 0) + 
                _get(aData, "status.canceled", 0) +
                failureTotal(request) +
                queuedTotal(request) +
                submittedTotal(request));
    }
    
    function failureTotal(request) {
        if (typeof(request) == "string") {
            var aData = getDataByWorkflow(request);
        } else {
            var aData = request;
        }
        return (_get(aData, "status.failure.create", 0) + 
                _get(aData, "status.failure.submit", 0) + 
                _get(aData, "status.failure.exception", 0));
    };
    
    function queuedTotal(request) {
        if (typeof(request) == "string") {
            var aData = getDataByWorkflow(request);
        } else {
            var aData = request;
        }
        return (_get(aData, "status.queued.first", 0) + 
                _get(aData, "status.queued.retry", 0));
    };
    
    function estimateCompletionTime(request) {
        //TODO need to improve the algo
        // no infomation to calulate the estimate completion time
        var aData = getDataByWorkflow(request);
        var completedJobs = _get(aData, "status.success", 0) + failureTotal(request);
        if (completedJobs == 0) return -1;
        // get running start time.
        var requestStatus = _get(aData, "request_status");
        var lastStatus = requestStatus[requestStatus.length - 1];
        
        //request is done
        if (lastStatus.status !== 'running') return 0;
        
        var totalJobs = getWMBSJobsTotal(request) - _get(aData, "status.canceled", 0);
        // jobCompletion percentage 
        var completionRatio = (completedJobs / totalJobs);
        var queueInjectionRatio = _get(aData, "status.inWMBS",  0) / _get(aData, 'total_jobs', 1);
        var duration = Math.round(Date.now() / 1000) - lastStatus.update_time;
        var timeLeft = Math.round((duration / (completionRatio * queueInjectionRatio)) - duration);
        
        return timeLeft;
    }
    
    function submittedTotal(request) {
        if (typeof(request) == "string") {
            var aData = getDataByWorkflow(request);
        } else {
            var aData = request;
        }
        return (_get(aData, "status.submit.first", 0) + 
                _get(aData, "status.submit.retry", 0));
    };

    function getSummary() {
        return _summary;
    }
    
    function getFilteredSummary() {
        return _filteredSummary;
    }
    
    return {'getDataByWorkflow': getDataByWorkflow,
            'updateBulkRequests': updateBulkRequests,
            'updateRequest': updateRequest,
            'getList': getList,
            'getLength': getLength,
            'getWMBSJobsTotal': getWMBSJobsTotal,
            'failureTotal': failureTotal,
            'queuedTotal': queuedTotal,
            'submittedTotal': submittedTotal,
            'filterRequests': filterRequests,
            'setFilter': setFilter,
            'setDataByWorkflow': setDataByWorkflow,
            'getSummary': getSummary,
            'getFilteredSummary': getFilteredSummary,
            'estimateCompletionTime': estimateCompletionTime
            }
}
