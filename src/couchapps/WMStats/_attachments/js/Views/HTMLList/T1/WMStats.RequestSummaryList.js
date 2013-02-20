WMStats.namespace('RequestSummaryList');
(function() { 
    var format = function (summary) {
        var summaryStruct = summary.summaryStruct
        htmlstr = "";
        htmlstr += "<legend>filtered stats</legend>";
        htmlstr += "<div class='requestSummaryBox'>";
        htmlstr += "<ul>";
        htmlstr += "<li><b>requests:</b> " + summary.summaryStruct.length + "</li>";
        htmlstr += "<li><b>total events:</b> " + summary.summaryStruct.totalEvents + "</li>";
        htmlstr += "<li><b>processed events:</b> " + summary.getAvgEvents() + "</li>";
        htmlstr += "<li><b>created:</b> " + summary.getWMBSTotalJobs() + "</li>";
        htmlstr += "<li><b>cooloff:</b> " + summary.getTotalCooloff() + "</li>";
        htmlstr += "<li><b>success:</b> " + summary.getJobStatus('success') + "</li>";
        htmlstr += "<li><b>failure:</b> " + summary.getTotalFailure() + "</li>";
        htmlstr += "<li><b>queued:</b> " + summary.getTotalQueued() + "</li>";
        htmlstr += "<li><b>running:</b> " + summary.getJobStatus('submitted.running') + "</li>";
        htmlstr += "<li><b>pending:</b> " + summary.getJobStatus('submitted.pending') + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestSummaryList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})();
