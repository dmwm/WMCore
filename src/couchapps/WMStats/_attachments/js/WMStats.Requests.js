WMStats.namespace("Requests");
WMStats.Requests = function () {
    /*
     * Data structure for holding the request
     */
    // request data by workflow name
    var _dataByWorkflow = {};
    var _dataByWorkflowAgent = {}
    // number of requests in the data
    var length = 0;
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
    
    function updateRequest(doc) {
        var request = getRequestByName(doc.workflow);
        if (!request) {
            request = {}; 
            length++;
            _dataByWorkflow[doc.workflow] = request;
        }
        var requestWithAgent = getRequestByNameAndAgent(doc.workflow, doc.agent_url);
         
        for (var field in doc) {
            //handles when request is splited in more than one agents
            if (request[field]  && (field == 'sites' || field == 'status')){
                _addJobs(request[field], doc[field])
            } else {
                request[field] = doc[field];
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
    
    function getRequestByName(workflow) {
        return _dataByWorkflow[workflow];
    };
    
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
    
    function getDataByWorkflow() {
        return _dataByWorkflow;
    };
    
    function getList() {
        var list = [];
        for (var item in _dataByWorkflow) {
            list.push(_dataByWorkflow[item])
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
    
    return {'getDataByWorkflow': getDataByWorkflow,
            'updateBulkRequests': updateBulkRequests,
            'updateRequest': updateRequest,
            'getRequestByName': getRequestByName,
            'getList': getList,
            'length': length
            }
}
