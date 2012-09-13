WMStats.namespace('RequestDataList');
(function() { 
    var format = function (summary) {
        var summaryStruct = summary.summaryStruct
        htmlstr = "";
        htmlstr += "<div class='requestInfoBox' id='requestDetail-0'>"
        htmlstr += "<ul>";
        htmlstr += "<li> requests: " + summary.summaryStruct.length + "</li>";
        htmlstr += "<li> total created jobs: " + summary.getWMBSTotalJobs() + "</li>";
        htmlstr += "<li> total events: " + summary.summaryStruct.totalEvents + "</li>";
        htmlstr += "<li> processed events: " + summary.summaryStruct.processedEvents + "</li>";
        htmlstr += "<li> success jobs: " + summary.getJobStatus('success') + "</li>";
        htmlstr += "<li> failure jobs: " + summary.getTotalFailure() + "</li>";
        htmlstr += "<li> queued jobs: " + summary.getTotalQueued() + "</li>";
        htmlstr += "<li> running jobs: " + summary.getJobStatus('submit.running') + "</li>";
        htmlstr += "<li> pending jobs: " + summary.getJobStatus('submit.pending') + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDataList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()