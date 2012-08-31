WMStats.namespace('RequestDetailList');
(function() { 
    var format = function (requestStruct) {
        var htmlstr = "";
        var reqDoc = requestStruct.request;
        var reqSummary = requestStruct.summary;
        
        //var allRequests.getDataByWorkflow =  allRequests.getDataByWorkflow
        htmlstr += "<div class='requestInfoBox' id='requestDetail-0'>"
        htmlstr += "<ul>";
        if (reqDoc) {
            htmlstr += "<li> workflow: " + reqDoc.workflow + "</li>";
            htmlstr += "<li> queued (first): " + reqSummary.getJobStatus("queued.first", 0) + "</li>";
            htmlstr += "<li> queued (retried): " + reqSummary.getJobStatus("queued.retry", 0) + "</li>";
            htmlstr += "<li> pending: " + reqSummary.getJobStatus("submitted.pending", 0) + "</li>";
            htmlstr += "<li> running: " + reqSummary.getJobStatus("submitted.running", 0) + "</li>";
            htmlstr += "<li> failure: " + reqSummary.getTotalFailure()  + "</li>";
            htmlstr += "<li> success: " + reqSummary.getJobStatus("success", 0) + "</li>";
        }
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()
