WMStats.namespace('RequestDataList');
(function() { 
    var format = function (summary) {
        var summaryStruct = summary.summaryStruct
        htmlstr = "";
        htmlstr += "<div class='requestSummaryBox'>"
        htmlstr += "<ul>";
        htmlstr += "<li> requests: " + summary.summaryStruct.length + "</li>";
        htmlstr += "<li> total created jobs: " + summary.getWMBSTotalJobs() + "</li>";
        htmlstr += "<li> paused jobs: " + summary.getTotalPaused() + "</li>";
        htmlstr += "<li> cooloff jobs: " + summary.getTotalCooloff() + "</li>";
        htmlstr += "<li> success jobs: " + summary.getJobStatus('success') + "</li>";
        htmlstr += "<li> failure jobs: " + summary.getTotalFailure() + "</li>";
        htmlstr += "<li> queued jobs: " + summary.getTotalQueued() + "</li>";
        htmlstr += "<li> running jobs: " + summary.getJobStatus('submitted.running') + "</li>";
        htmlstr += "<li> pending jobs: " + summary.getJobStatus('submitted.pending') + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDataList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()