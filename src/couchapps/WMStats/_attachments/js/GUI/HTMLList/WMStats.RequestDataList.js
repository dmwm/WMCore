WMStats.namespace('RequestDataList');
(function() { 
    var format = function (summary) {
        htmlstr = "";
        htmlstr += "<div class='requestInfoBox' id='requestDetail-0'>"
        htmlstr += "<ul>";
        htmlstr += "<li> requests: " + summary.length + "</li>";
        htmlstr += "<li> total created jobs: " + summary.totalJobs + "</li>";
        htmlstr += "<li> total events: " + summary.totalEvents + "</li>";
        htmlstr += "<li> processed events: " + summary.processedEvents + "</li>";
        htmlstr += "<li> success jobs: " + summary.success + "</li>";
        htmlstr += "<li> failure jobs: " + summary.failure + "</li>";
        htmlstr += "<li> queued jobs: " + summary.quequed + "</li>";
        htmlstr += "<li> running jobs: " + summary.running + "</li>";
        htmlstr += "<li> pending jobs: " + summary.pending + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    }
    
    WMStats.RequestDataList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()