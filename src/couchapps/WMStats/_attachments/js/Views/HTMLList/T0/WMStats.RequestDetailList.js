WMStats.namespace('RequestDetailList');
(function() { 
    
    var vm = WMStats.ViewModel;
    var transitionFormat = function(dataArray, maxLength, summaryStr) {
        var htmlstr = "";
        if (dataArray == undefined || dataArray.length == undefined ||
            dataArray.length <= maxLength) {
         
            htmlstr +=  dataArray;
         } else {
            htmlstr += "<details> <summary>" + summaryStr +"</summary><ul>";  
            for (var i in dataArray) {
                htmlstr += "<li> <b>" + dataArray[i].status + ":</b> " + WMStats.Utils.utcClock(new Date(dataArray[i].update_time * 1000)) + "</li>";
            }
            htmlstr += "</ul></details>";
        }
        return htmlstr;
    };
    
    var format = function (requestStruct) {
        var htmlstr = '<div class="closingButton">X</div>';
        var reqDoc = requestStruct.requests;
        var reqSummary = requestStruct.summary;
        
        htmlstr += "<div class='requestDetailBox'>";
        htmlstr += "<ul>";
        if (reqDoc) {
            
            htmlstr += "<li><b>category:</b> " + requestStruct.key + "</li>";
            htmlstr += "<li><b>state transition</b> " + transitionFormat(reqDoc[requestStruct.key].request_status, 0, "State List") + "</li>";
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
            if (reqDoc.skipped) {
        		htmlstr += "<li>" + WMStats.Utils.expandFormat(reqDoc.getSkippedDetail(), "Skipped Summary", siteStatusFormat) + "</li>";
        	}
        }
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    WMStats.RequestDetailList = function (workflow, containerDiv) {
        var allRequests = vm.ActiveRequestPage.data();
        var reqDoc = allRequests.getData(workflow);
        var reqSummary = allRequests.getSummary(workflow);
        var requests = {};
        requests[workflow] = reqDoc;
        var data = {key: workflow, requests: requests, summary: reqSummary};
        $(containerDiv).html(format(data));
        $(containerDiv).show("slide", {}, 500);
        vm.RequestDetail.open = true;
    };
    
    var vm = WMStats.ViewModel;

    vm.RequestDetail.subscribe("requestName", function() {
        WMStats.RequestDetailList(vm.RequestDetail.requestName(), vm.RequestDetail.id());
    });
})();
