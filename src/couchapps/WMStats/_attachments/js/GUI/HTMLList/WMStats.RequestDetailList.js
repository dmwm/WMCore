WMStats.namespace('RequestDetailList');
(function() { 
    var format = function (workflowName) {
        var htmlstr = "";
        var allRequests = WMStats.ActiveRequestView.getData();
        var reqDoc = allRequests.getDataByWorkflow(workflowName);
        var _get =  allRequests.getDataByWorkflow
        htmlstr += "<div class='requestInfoBox' id='requestDetail-0'>"
        htmlstr += "<ul>";
        htmlstr += "<li> campaign: " + reqDoc.campaign + "</li>";
        htmlstr += "<li> workflow: " + reqDoc.workflow + "</li>";
        htmlstr += "<li> request date: " + reqDoc.request_date + "</li>";
        htmlstr += "<li> request type: " + reqDoc.request_type + "</li>";
        htmlstr += "<li> input dataset: " + reqDoc.inputdataset + "</li>";
        htmlstr += "<li> input events: " + reqDoc.input_events + "</li>";
        htmlstr += "<li> output events: " + _get(workflowName, "output_progress.0.events", 0) + "</li>";
        htmlstr += "<li> failure: " + allRequests.failureTotal(workflowName)  + "</li>";
        htmlstr += "<li> success: " + _get(workflowName, "status.success", 0) + "</li>";
        htmlstr += "<li> site whitelist: " + reqDoc.site_white_list + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()
