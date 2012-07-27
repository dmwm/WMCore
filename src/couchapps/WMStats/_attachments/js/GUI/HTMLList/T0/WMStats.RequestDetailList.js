WMStats.namespace('RequestDetailList');
(function() { 
    var format = function (workflowName) {
        var htmlstr = "";
        var allRequests = WMStats.ActiveRequestView.getData();
        var reqDoc = allRequests.getDataByWorkflow(workflowName);
        var _get =  allRequests.getDataByWorkflow
        htmlstr += "<div class='requestInfoBox' id='requestDetail-0'>"
        htmlstr += "<ul>";
        if (reqDoc) {
            htmlstr += "<li> workflow: " + reqDoc.workflow + "</li>";
            htmlstr += "<li> queued (first): " + _get(workflowName, "status.queued.first", 0) + "</li>";
            htmlstr += "<li> queued (retried): " + _get(workflowName, "status.queued.retry", 0) + "</li>";
            htmlstr += "<li> pending: " + _get(workflowName, "status.submitted.pending", 0) + "</li>";
            htmlstr += "<li> running: " + _get(workflowName, "status.submitted.running", 0) + "</li>";
            htmlstr += "<li> failure: " + allRequests.failureTotal(workflowName)  + "</li>";
            htmlstr += "<li> success: " + _get(workflowName, "status.success", 0) + "</li>";
        }
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()
