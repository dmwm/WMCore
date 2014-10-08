WMStats.namespace("RequestAlertGUI");

WMStats.RequestAlertGUI = function (requestData, containerDiv) {
    
     function displayAlert(errorArray, title) {
        if (errorArray.length > 0) {
            htmlList += '<fieldset><legend>' + title + '</legend> <div><ul>';
        
            for (var i in errorArray) {
                var workflow = errorArray[i].getName();
                var summary = errorArray[i].getSummary();
                var running = summary.getRunning();
                var pending = summary.getPending();
                var cooloff = summary.getTotalCooloff();
                var failure =summary.getTotalFailure();
                var success = summary.getJobStatus("success");
                var lastState = errorArray[i].getLastStateAndTime();
                var lastStatus = "N/A";
                var lastUpdate = "N/A";
                if (lastState !== null) {
                	lastStatus = lastState.status;
                	lastUpdate = WMStats.Utils.utcClock(new Date(lastState.update_time * 1000));
                } 
                htmlList += ('<li> <a class="requestAlert">' + workflow + "</a>: status:" + 
                             lastStatus + " (" + lastUpdate + "), cooloff " + cooloff + 
                             " failure:" + failure + " success:" + success +
                             " running:" + running + " pending:" + pending + '</li>');
            }
            htmlList += "</ul></div></fieldset>";
            
            errorFlag = true;
        };
    };
    
    
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
    
    var alertRequests = requestData.getRequestAlerts();
    var errorFlag = false;
    if (alertRequests.length > 0) {
        alertRequests = alertRequests.sort(alertSort);
    };    
    var configError = alertRequests.configError;
    var siteError = alertRequests.siteError;
    var paused = alertRequests.paused;
    
    var htmlList = "";
    
    displayAlert(configError, "Config Error");
    displayAlert(siteError, "Site Error");
    displayAlert(paused, "paused");
    
    $(containerDiv).addClass("request_error_box");
    if (errorFlag) {
        $(containerDiv).removeClass("stable warning").addClass("error").html(htmlList);
    } else {
        $(containerDiv).removeClass("warning error").addClass("stable").html("request alarm");
    }
};

// controller for request alert view
(function() {
    
     var vm = WMStats.ViewModel;
     vm.RequestAlertPage.subscribe("data", function(){
        var divSelector = vm.RequestAlertPage.id() + " div.summary_data";
        WMStats.RequestAlertGUI(vm.RequestAlertPage.data(), divSelector);
     });
    
     $(document).on('click', 'a.requestAlert', function() {
        var workflow = $(this).text();
        
        vm.ActiveRequestPage.view(vm.JobView);
        vm.page(vm.ActiveRequestPage);
        vm.JobView.requestName(workflow);
        $(this).addClass('reviewed');
    });
})();

