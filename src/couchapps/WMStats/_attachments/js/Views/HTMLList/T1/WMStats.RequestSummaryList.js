WMStats.namespace('RequestSummaryList');
(function() { 
    var numFormat = WMStats.Utils.largeNumberFormat;
    var format = function (summary) {
        var summaryStruct = summary.summaryStruct;
        var htmlstr = "";
        htmlstr += "<legend>filtered stats</legend>";
        htmlstr += "<div class='requestSummaryBox'>";
        htmlstr += "<ul>";
        htmlstr += "<li><b>requests:</b> " + summary.summaryStruct.length + "</li>";
        htmlstr += "<li><b>total events:</b> " + numFormat(summary.summaryStruct.totalEvents) + "</li>";
        htmlstr += "<li><b>processed events:</b> " + numFormat(summary.getAvgEvents()) + "</li>";
        htmlstr += "<li><b>created:</b> " + numFormat(summary.getWMBSTotalJobs()) + "</li>";
        htmlstr += "<li><b>cooloff:</b> " + numFormat(summary.getTotalCooloff()) + "</li>";
        htmlstr += "<li><b>success:</b> " + numFormat(summary.getJobStatus('success')) + "</li>";
        htmlstr += "<li><b>failure:</b> " + numFormat(summary.getTotalFailure()) + "</li>";
        htmlstr += "<li><b>queued:</b> " + numFormat(summary.getTotalQueued()) + "</li>";
        htmlstr += "<li><b>running:</b> " + numFormat(summary.getJobStatus('submitted.running')) + "</li>";
        htmlstr += "<li><b>pending:</b> " + numFormat(summary.getJobStatus('submitted.pending')) + "</li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    WMStats.RequestSummaryList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    };
    
    var vm = WMStats.ViewModel;
    
    vm.ActiveRequestPage.subscribe("data", function() {
            var filteredData = vm.ActiveRequestPage.data();
            WMStats.RequestSummaryList(filteredData.getSummary(), "#filter_summary");
       });
})();
