WMStats.namespace('WMStats.RequestTitle');
(function() { 
    var format = function (data) {
        var workflow;
        var dataType;
        if (typeof data === "string") {
            workflow = data;
            dataType = 0;
        } else {
            workflow = data.getData().workflow;
            dataType = 1;
        };
        var requestInfo = WMStats.ActiveRequestModel.getData().getData(workflow);

        var htmlstr = "";
        //htmlstr += "<legend>request</legend>";
        htmlstr += "<div class='requestSummaryBox'>";
        htmlstr += "<ul>";
        htmlstr += "<li><b>" + workflow + "</b></li>";
        if (dataType == 1) {
            htmlstr += "<li><b>agent: </b>" + requestInfo.agent_url + "</li>";
        };
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    WMStats.RequestTitle = function (data, containerDiv) {
        if (typeof data === "string" || (typeof data === "object" && data.getData().workflow !== undefined)) {
            $(containerDiv).html(format(data));
        };
    };
})();
