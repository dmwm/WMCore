WMStats.namespace('RequestDetailList');
(function() { 
    
    var progressFormat = function(progressStat, totalEvents, totalLumis) {
        var formatStr = "<ul>";
        for (var output in progressStat) {
            var processedEvents = progressStat[output].events || 0;
            var processedLumis = progressStat[output].totalLumis || 0;
            var eventPercent = (processedEvents / totalEvents * 100).toFixed(1) + '%';
            var lumiPercent = (processedLumis / totalLumis * 100).toFixed(1) + '%';
            formatStr += "<li><b>" + output + ": event:</b> " + eventPercent + ",  <b>lumi:</b> " + lumiPercent + "</li>";
        }
        formatStr += "</ul>";
        return formatStr;
    };
    
    var expandFormat = function(dataArray, maxLength, summaryStr) {
        var htmlstr = "";
        if (dataArray == undefined || dataArray.length == undefined ||
            dataArray.length <= maxLength) {
         
            htmlstr +=  dataArray;
         } else {
            htmlstr += "<details> <summary>" + summaryStr +"</summary><ul>"  
            for (var i in dataArray) {
                htmlstr += "<li>" + dataArray[i] + "</li>";
            }
            htmlstr += "</ul></details>";
        }
        return htmlstr;
    }
    
    var format = function (requestStruct) {
        var htmlstr = '<div class="closingButton">X</div>';
        var reqDoc = requestStruct.requests[requestStruct.key];
        var reqSummary = requestStruct.summary;
        
        htmlstr += "<div class='requestDetailBox'>"
        htmlstr += "<ul>";
        if (reqDoc) {
            htmlstr += "<li><b>campaign:</b> " + reqDoc.campaign + "</li>";
            htmlstr += "<li><b>workflow:</b> " + WMStats.Utils.formatReqDetailUrl(reqDoc.workflow) + "</li>";
            htmlstr += "<li><b>agent url:</b> " + reqDoc.agent_url + "</li>";
            htmlstr += "<li><b>prep id:</b> " + reqDoc.prep_id + "</li>";
            htmlstr += "<li><b>teams:</b> " + reqDoc.teams + "</li>";
            htmlstr += "<li><b>requetor:</b> " + reqDoc.requestor + "</li>";
            htmlstr += "<li><b>request date:</b> " + reqDoc.request_date + "</li>";
            htmlstr += "<li><b>request type:</b> " + reqDoc.request_type + "</li>";
            htmlstr += "<li><b>CMSSW:</b> " + reqDoc.cmssw + "</li>";
            htmlstr += "<li><b>user dn:</b> " + reqDoc.user_dn + "</li>";
            htmlstr += "<li><b>vo role:</b> " + reqDoc.vo_role + "</li>";
            htmlstr += "<li><b>vo group:</b> " + reqDoc.vo_group + "</li>";
            htmlstr += "<li><b>status:</b> " + WMStats.Utils.formatWorkloadSummarylUrl(reqDoc.workflow, 
                                                 reqDoc.request_status[reqDoc.request_status.length - 1].status) + "</li>";
            htmlstr += "<li><b>input dataset:</b> " + reqDoc.inputdataset + "</li>";
            htmlstr += "<li><b>input events:</b> " + reqDoc.input_events + "</li>";
            htmlstr += "<li><b>site white list:</b> " + expandFormat(reqDoc.site_white_list, 4, "Multiple Sites") + "</li>";
            htmlstr += "<li><b>output datasets:</b> " + expandFormat(reqDoc.outputdatasets, 1, "Multiple Datasets") + "</li>";
            htmlstr += "<li><b>progress:</b> " + progressFormat(reqDoc.getProgressStat(), Number(reqDoc.input_events), Number(reqDoc.input_lumis))  + "</li>";
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
    }
    
    WMStats.RequestDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})();
