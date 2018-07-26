WMStats.namespace('CategoryDetailList');
(function() { 
    var format = function (requestStruct) {
        var htmlstr = '';
        var reqDoc = requestStruct.requests[requestStruct.key];
        var reqSummary = requestStruct.summary;
        
        htmlstr += "<div class='requestDetailBox'>";
        htmlstr += "<ul>";
        if (reqDoc) {
            htmlstr += "<li><b>campaign:</b> " + reqDoc.campaign + "</li>";
            htmlstr += "<li><b>workflow:</b> " + WMStats.Utils.formatReqDetailUrl(reqDoc.workflow) + "</li>";
            htmlstr += "<li><b>requestor:</b> " + reqDoc.requestor + "</li>";
            htmlstr += "<li><b>request date:</b> " + reqDoc.request_date + "</li>";
            htmlstr += "<li><b>request type:</b> " + reqDoc.request_type + "</li>";
            htmlstr += "<li><b>CMSSW:</b> " + reqDoc.cmssw + "</li>";
            htmlstr += "<li><b>user dn:</b> " + reqDoc.user_dn + "</li>";
            //htmlstr += "<li><b>vo role:</b> " + reqDoc.vo_role + "</li>";
            //htmlstr += "<li><b>vo group:</b> " + reqDoc.vo_group + "</li>";
            htmlstr += "<li><b>status:</b> " + WMStats.Utils.formatWorkloadSummarylUrl(reqDoc.workflow, 
                                                 reqDoc.request_status[reqDoc.request_status.length - 1].status) + "</li>";
            htmlstr += "<li><b>input dataset:</b> " + WMStats.Utils.getInputDatasets(reqDoc) + "</li>";
            htmlstr += "<li><b>input events:</b> " + reqDoc.input_events + "</li>";
            htmlstr += "<li><b>input lumis:</b> " + reqDoc.input_lumis + "</li>";
            htmlstr += "<li><b>input files:</b> " + reqDoc.input_num_files + "</li>";
            htmlstr += "<li><b>site white list:</b> " + reqDoc.site_white_list + "</li>";
            htmlstr += "<li><b>output datasets:</b> " + reqDoc.outputdatasets + "</li>";
        }
        if (reqSummary) {
            htmlstr += "<li><b>output events:</b> " + reqSummary.summaryStruct.processedEvents + "</li>";
            htmlstr += "<li><b>queued (first):</b> " + reqSummary.getJobStatus("queued.first", 0) + "</li>";
            htmlstr += "<li><b>queued (retried):</b> " + reqSummary.getJobStatus("queued.retry", 0) + "</li>";
            htmlstr += "<li><b>cooloff jobs:</b> " + reqSummary.getTotalCooloff() + "</li>";
            htmlstr += "<li><b>pending:</b> " + reqSummary.getJobStatus("submitted.pending", 0) + "</li>";
            htmlstr += "<li><b>running:</b> " + reqSummary.getJobStatus("submitted.running", 0) + "</li>";
            htmlstr += "<li><b>failure:</b>" + reqSummary.getTotalFailure()  + "</li>";
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
