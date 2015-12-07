WMStats.namespace('CategoryDetailList');
(function() { 
    var format = function (requestStruct) {
        var htmlstr = '';
        var reqDoc = requestStruct.requests;
        var reqSummary = requestStruct.summary;
        
        htmlstr += "<div class='requestDetailBox'>";
        htmlstr += "<ul>";
        if (reqDoc) {
            
            htmlstr += "<li><b>category:</b> " + requestStruct.key + "</li>";
            htmlstr += "<li><b>queued (first):</b> " + reqSummary.getJobStatus("queued.first", 0) + "</li>";
            htmlstr += "<li><b>queued (retried):</b> " + reqSummary.getJobStatus("queued.retry", 0) + "</li>";
            htmlstr += "<li><b>created:</b> " + reqSummary.getWMBSTotalJobs() + "</li>";
            htmlstr += "<li><b>paused jobs:</b> " + reqSummary.getTotalPaused() + "</li>";
            htmlstr += "<li><b>cooloff jobs:</b> " + reqSummary.getTotalCooloff() + "</li>";
            htmlstr += "<li><b>submitted:</b> " + reqSummary.getTotalSubmitted() + "</li>";
            htmlstr += "<li><b>pending:</b> " + reqSummary.getJobStatus("submitted.pending", 0) + "</li>";
            htmlstr += "<li><b>running:</b> " + reqSummary.getJobStatus("submitted.running", 0) + "</li>";
            htmlstr += "<li><b>failure:</b> " + reqSummary.getTotalFailure()  + "</li>";
            htmlstr += "<li><b>success:</b> " + reqSummary.getJobStatus("success", 0) + "</li>";
        }
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    WMStats.CategoryDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    };
    
    var vm = WMStats.ViewModel;
    
    vm.CategoryDetail.subscribe("data", function() {
        WMStats.CategoryDetailList(vm.CategoryDetail.data(), vm.CategoryDetail.id());
    });
})();
