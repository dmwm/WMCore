WMStats.namespace("Requests");
WMStats.Requests = function () {
    /*
     * Data structure for holding the request
     */
    // request data by workflow name
    var _dataByWorkflow = {};
    // number of requests in the data
    var length = 0;
    
    function updateRequest(doc) {
        var request = getRequestByName(doc.workflow);
        if (!request) {
            request = {}; 
            length++;
            _dataByWorkflow[doc.workflow] = request;
        } 
        for (var field in doc) {
            request[field] = doc[field];
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
    
    function getDataByWorkflow() {
        return _dataByWorkflow;
    };
    
    function getList() {
        var list = [];
        for (var item in _dataByWorkflow) {
            list.push(_dataByWorkflow[item])
        }
        return list;
    }
        
    return {'getDataByWorkflow': getDataByWorkflow,
            'updateBulkRequests': updateBulkRequests,
            'updateRequest': updateRequest,
            'getRequestByName': getRequestByName,
            'getList': getList,
            'length': length     
            }    
}
