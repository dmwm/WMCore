WMStats.namespace("Requests");
WMStats.Requests = function () {
    var _dataByWorkflow = {};
    var length = 0;
    
    function updateRequest(doc) {
        //if !(doc.workflow) {throw};
        var request = getRequest(doc.workflow);
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
            updateRequest(row.doc);
        }
    };
    
    function getRequestByName(workflow) {
        return _dataByWorkflow['workflow'];
    };
    
    function getDataByWorkflow() {
        return _dataByWorkflow;
    };
        
    return {'getDataByRequest': getDataByRequest,
            'updateBulkRequests': updateBulkRequests,
            'updateRequest': updateRequest,
            'getRequestByName': getRequestByName,
            'length': length     
            }    
}
