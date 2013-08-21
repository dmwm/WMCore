WMStats.namespace("AgentDetailList");

(function () {
    
    var statusInterpretator = function(alertStatus) {
        if (alertStatus == "agent_down") {
            message = "Data is not updated: AnalyticsDataCollector Down";
        } else if (alertStatus == "component_down") {
            message = "Components or Thread down";
        } else if (alertStatus == "drain_mode") {
            message = "Draining Agent";
        } else {
            message = "OK";
        };
        return message;
    };
    
    var componentFormat = function(componentList) {
        var formatStr = "";
        for (var i in componentList) {
            formatStr += "<details> <summary>" + componentList[i].name +"</summary> <ul>";
            formatStr += "<li><b>" + componentList[i].worker_name +"</b> </li>";
            formatStr += "<li><b>status</b>: " + componentList[i].state + "</li>";
            formatStr += "<li><b>error</b>: " + WMStats.Utils.utcClock(new Date(componentList[i].last_error * 1000)) + "</li>";
            formatStr += "<li><b>error message</b>: <pre>" + componentList[i].error_message + "</pre></li>";
            //formatStr += "<li><b>time</b>: " + WMStats.Utils.utcClock(new Date(componentList[i].last_updated * 1000)) + "</li>";
            formatStr += "<li><b>pid</b>: " + componentList[i].pid + "</li>";
            formatStr += "</ul></details>";
        }
        
        return formatStr;
    };
    
    var agentErrorFormat = function (agentInfo) {
        var htmlstr = '';
        htmlstr += "<div class='error agent_detail_box'>";
        htmlstr += "<ul>";
        if (agentInfo) {
            htmlstr += "<li><b>agent:</b> " + agentInfo.agent_url + "</li>";
            htmlstr += "<li><b>last_updated:</b> " + WMStats.Utils.utcClock(new Date(agentInfo.timestamp * 1000)) +" : " + 
                              agentInfo.alert.message  + "</li>";
            htmlstr += "<li><b>status:</b> " + statusInterpretator(agentInfo.alert.status) + "</li>";
            htmlstr += "<li><b>team:</b> " + agentInfo.agent_team+ "</li>";
        };
        var detailInfo = agentInfo.down_component_detail;
        if (detailInfo && (detailInfo.length > 0)) {
            htmlstr += "<li><b>component errors:</b> ";
            htmlstr += componentFormat(detailInfo);
            htmlstr +="</li>";
           };
        
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    var agentStableFormat = function (agentInfo) {
        var htmlstr = '';
        htmlstr += "<div class='stable agent_detail_box'>";
        htmlstr += "<ul>";
        if (agentInfo) {
            htmlstr += "<li><b>agent:</b> " + agentInfo.agent_url + "</li>";
            htmlstr += "<li><b>last_updated:</b> " + agentInfo.alert.message  + "</li>";
            htmlstr += "<li><b>status:</b> " + statusInterpretator(agentInfo.alert.status) + "</li>";
            htmlstr += "<li><b>team</b> " + agentInfo.agent_team+ "</li>";
        }
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    var format = function (agentData) {
       var htmlstr = "";
       var agentsWithWarning = agentData.getAlertList();
       for (i in agentsWithWarning) {
           if (agentsWithWarning[i].alert && agentsWithWarning[i].alert.status !== "ok") {
               htmlstr += agentErrorFormat(agentsWithWarning[i]);
           } else {
               htmlstr += agentStableFormat(agentsWithWarning[i]);
           };
           
       };
       return htmlstr;
    };
    
    WMStats.AgentDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    };
    
    // controller for this view to be triggered
    var vm = WMStats.ViewModel;
    vm.AgentPage.subscribe("data", function() {
        //TODO get id form the view
        var divID = '#agent_detail';
        WMStats.AgentDetailList(vm.AgentPage.data(), divID);
    });
        
})();
