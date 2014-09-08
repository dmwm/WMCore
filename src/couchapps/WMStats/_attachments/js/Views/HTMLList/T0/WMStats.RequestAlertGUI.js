WMStats.namespace("RequestAlertGUI");

WMStats.RequestAlertGUI = function (requestData, containerDiv) {
    
    function mapType(workflowName){
        // TODO: this is a hack to get jobType rely on the conventsion
        // need a better solution
        var workflowType = workflowName.split("_")[0].toLowerCase();
        if (workflowType == "express") {
            return 1;
        } else if (workflowType == "repack") {
            return 2;
        } else if (workflowType == "promptreco") {
            return 3;
        }
        return 4;
    }
    
    function alertSort(a, b) {
        var aRun = a.requests[a.key].run;
        var bRun = b.requests[b.key].run;
        if (aRun == bRun) {
            var aType = mapType(a.key);
            var bType = mapType(b.key);
            if ( aType == bType){
                var aJobs = a.summary.getTotalPaused() + a.summary.getTotalCooloff();
                var bJobs = b.summary.getTotalPaused() + b.summary.getTotalCooloff();
                return bJobs - aJobs;
            } else {
                return aType - bType;
            }
        } else {
            return aRun - bRun;
        }
    }
    
    var alertRequests = requestData.getAlertRequests();
    
    if (alertRequests.length > 0) {
        var htmlList = "<ul>";
        alertRequests = alertRequests.sort(alertSort);
        for (var i in alertRequests) {
            var key = alertRequests[i].key;
            var summary = alertRequests[i].summary;
            var jobs = summary.getTotalPaused() + summary.getTotalCooloff();
            //var reqDoc = alertRequests[i].requests[key];
            htmlList += ('<li> <a class="requestAlert">' + key + "</a>:" + jobs + '</li>');
        }
        htmlList += "</ul>";
        
        $(containerDiv).removeClass("stable warning").addClass("error").html(htmlList);
    } else {
        $(containerDiv).removeClass("warning error").addClass("stable").html("request alarm");
    }
};
