WMStats.namespace('RequestDetailList');
(function() { 
    var format = function (requestStruct) {
        var htmlstr = "";
        var reqDoc = requestStruct.requests[requestStruct.key];
        var reqSummary = requestStruct.summary;
        
        htmlstr += "<div class='requestDetailBox'>"
        htmlstr += "<ul>";
        if (reqDoc) {
            htmlstr += "<li> campaign: " + reqDoc.campaign + "</li>";
            htmlstr += "<li> workflow: " + reqDoc.workflow + "</li>";
            htmlstr += "<li> requetor: " + reqDoc.requestor + "</li>";
            htmlstr += "<li> request date: " + reqDoc.request_date + "</li>";
            htmlstr += "<li> request type: " + reqDoc.request_type + "</li>";
            htmlstr += "<li> user dn: " + reqDoc.user_dn + "</li>";
            htmlstr += "<li> vo role: " + reqDoc.vo_role + "</li>";
            htmlstr += "<li> vo group: " + reqDoc.vo_group + "</li>";
            htmlstr += "<li> input dataset: " + reqDoc.inputdataset + "</li>";
            htmlstr += "<li> input events: " + reqDoc.input_events + "</li>";
            htmlstr += "<li> site white list: " + reqDoc.site_white_list + "</li>";
        }
        if (reqSummary) {
            htmlstr += "<li> output events: " + reqSummary.summaryStruct.processedEvents + "</li>";
            htmlstr += "<li> queued (first): " + reqSummary.getJobStatus("queued.first", 0) + "</li>";
            htmlstr += "<li> queued (retried): " + reqSummary.getJobStatus("queued.retry", 0) + "</li>";
            htmlstr += "<li> cooloff jobs: " + reqSummary.getTotalCooloff() + "</li>";
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
