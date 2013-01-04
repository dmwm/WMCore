WMStats.namespace("RequestAlertGUI");

WMStats.RequestAlertGUI = function (requestData, containerDiv) {

    var alertRequests = requestData.getAlertRequests();
    var notPulledRequests = requestData.requestNotPulledAlert();
    if ((alertRequests.length > 0) || (notPulledRequests.length > 0)) {
        var htmlList = "<ul>";
    
        for (var i in alertRequests) {
            var key = alertRequests[i].key;
            var summary = alertRequests[i].summary;
            var jobs = summary.getTotalPaused() + summary.getTotalCooloff()
            //var reqDoc = alertRequests[i].requests[key];
            htmlList += ('<li> <a class="requestAlert">' + key + "</a>:" + jobs + '</li>');
        }
        
        for (var i in notPulledRequests) {
            var key = notPulledRequests[i].request.key;
            htmlList += ('<li>' + key + ':' + notPulledRequests[i].message + '</li>');
        }
        
        htmlList += "</ul>";
        
        $(containerDiv).removeClass("stable warning").addClass("error").html(htmlList);
    } else {
        $(containerDiv).removeClass("warning error").addClass("stable").html("request alarm");
    }
};
