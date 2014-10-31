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
                var sPaused = summary.getJobStatus("paused.submit");
                var jPaused = summary.getJobStatus("paused.job");
                var cPaused = summary.getJobStatus("paused.create");
                var lastState = errorArray[i].getLastStateAndTime();
                var lastStatus = "N/A";
                var lastUpdate = "N/A";
                if (lastState !== null) {
                	lastStatus = lastState.status;
                	lastUpdate = WMStats.Utils.utcClock(new Date(lastState.update_time * 1000));
                } 
                htmlList += ('<li> <a class="requestAlert">' + workflow + "</a>: status:" + 
                             lastStatus + " (" + lastUpdate + "), cooloff " + cooloff + 
                             " submit paused:" + sPaused + " job paused:" + jPaused + 
                             " create paused:" + cPaused + 
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
        var aRun = a.run;
        var bRun = b.run;
        if (aRun == bRun) {
            var aType = mapType(a.workflow);
            var bType = mapType(b.workflow);
            if ( aType == bType){
                var aJobs = a.getSummary().getTotalPaused() + a.getSummary().getTotalCooloff();
                var bJobs = b.getSummary().getTotalPaused() + b.getSummary().getTotalCooloff();
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

    var htmlList = "";
    
    if (alertRequests.cPaused.length > 0) {
        var paused = alertRequests.cPaused.sort(alertSort);
        displayAlert(paused, "create paused");
    };
    if (alertRequests.sPaused.length > 0) {
        var paused = alertRequests.sPaused.sort(alertSort);
        displayAlert(paused, "submit paused");
    };
    if (alertRequests.jPaused.length > 0) {
        var paused = alertRequests.jPaused.sort(alertSort);
        displayAlert(paused, "job paused");
    };
    if (alertRequests.configError.length > 0) {
        var configError = alertRequests.configError.sort(alertSort);
        displayAlert(configError, "Config Error");
    };
    if (alertRequests.siteError.length > 0) {
        var siteError = alertRequests.siteError.sort(alertSort);
        displayAlert(siteError, "Site Error");
        
    };    

    
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
        vm.JobView.requestName(workflow);
        //TODO: this cause calling one more time for retrieving data
        vm.ActiveRequestPage.view(vm.JobView);
        vm.page(vm.ActiveRequestPage);
        
        $(this).addClass('reviewed');
    });
})();

