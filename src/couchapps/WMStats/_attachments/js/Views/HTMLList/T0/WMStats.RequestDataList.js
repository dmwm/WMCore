WMStats.namespace('RequestDataList');
(function() { 
    var format = function (summary) {
        var summaryStruct = summary.summaryStruct
        htmlstr = "";
        htmlstr += "<div class='requestSummaryBox'>"
        htmlstr += "<ul>";
        htmlstr += "<li> requests: " + summary.summaryStruct.length + "</li>";
        htmlstr += "<li> total created: " + summary.getWMBSTotalJobs() + "</li>";
        htmlstr += "<li> paused: " + summary.getTotalPaused() + "</li>";
        htmlstr += "<li> cooloff: " + summary.getTotalCooloff() + "</li>";
        htmlstr += "<li> success: " + summary.getJobStatus('success') + "</li>";
        htmlstr += "<li> failure: " + summary.getTotalFailure() + "</li>";
        htmlstr += "<li> queued: " + summary.getTotalQueued() + "</li>";
        htmlstr += "<li> running: " + summary.getJobStatus('submitted.running') + "</li>";
        htmlstr += "<li> pending: " + summary.getJobStatus('submitted.pending') + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDataList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()