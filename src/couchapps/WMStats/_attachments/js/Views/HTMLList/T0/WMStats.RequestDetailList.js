WMStats.namespace('RequestDetailList');
(function() { 
    var format = function (requestStruct) {
        var htmlstr = "";
        var reqDoc = requestStruct.requests;
        var reqSummary = requestStruct.summary;
        
        //var allRequests.getDataByWorkflow =  allRequests.getDataByWorkflow
        htmlstr += "<div class='requestDetailBox'>"
        htmlstr += "<ul>";
        if (reqDoc) {
            htmlstr += "<li> category: " + requestStruct.key + "</li>";
            htmlstr += "<li> queued (first): " + reqSummary.getJobStatus("queued.first", 0) + "</li>";
            htmlstr += "<li> queued (retried): " + reqSummary.getJobStatus("queued.retry", 0) + "</li>";
            htmlstr += "<li> created: " + reqSummary.getWMBSTotalJobs() + "</li>";
            htmlstr += "<li> paused jobs: " + reqSummary.getTotalPaused() + "</li>";
            htmlstr += "<li> cooloff jobs: " + reqSummary.getTotalCooloff() + "</li>";
            htmlstr += "<li> submitted: " + reqSummary.getTotalSubmitted() + "</li>"
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
