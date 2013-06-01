WMStats.namespace("RequestAlertGUI");

WMStats.RequestAlertGUI = function (requestData, containerDiv) {
    
    function displayAlert(errorArray, title) {
        if (errorArray.length > 0) {
            htmlList += '<fieldset><legend>' + title + '</legend> <div><ul>';
        
            for (var i in errorArray) {
                var workflow = errorArray[i].getName();
                var summary = errorArray[i].getSummary();
                var cooloff = summary.getTotalCooloff()
                var failure =summary.getTotalFailure();
                var success = summary.getJobStatus("success");
                htmlList += ('<li> <a class="requestAlert">' + workflow + "</a>: status:" + errorArray[i].getLastState() + ", cooloff " + cooloff + " failure:" + failure + " success:" + success + '</li>');
            }
            htmlList += "</ul></div></fieldset>";
            
            errorFlag = true;
        };
    };
    
    var alertRequests = requestData.getRequestAlerts();
    var notPulledRequests = requestData.requestNotPulledAlert();
    var configError = alertRequests.configError;
    var siteError = alertRequests.siteError;
    var failed = alertRequests.failed;
    var errorFlag = false;
    var numError = configError.length + siteError.length + failed.length;
    var htmlList = "";
    
    displayAlert(configError, "Config Error");
    displayAlert(siteError, "Site Error");
    displayAlert(failed, "failed");

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
