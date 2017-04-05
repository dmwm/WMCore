WMStats.namespace("AgentDetailList");

(function () {
    
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
    
    var diskFullFormat = function(diskList) {
        var formatStr = "<details> <summary> disk list</summary> <ul>";
        for (var i in diskList) {
            formatStr += "<li><b>" + diskList[i].mounted +"</b>:" + diskList[i].percent +"</li>";
        }
        formatStr += "</ul></details>";
        return formatStr;
    };
    
    var agentReportFormat = function (agentInfo, msgType) {
        var htmlstr = '';
        htmlstr += "<div class='" + msgType + " agent_detail_box'>";
        htmlstr += "<ul>";
        if (agentInfo) {
            agentVersion = '  (' + agentInfo.agent_version + ')'
            htmlstr += "<li><b>agent:</b> " + agentInfo.agent_url + agentVersion + "</li>";
            htmlstr += "<li><b>agent last updated:</b> " + WMStats.Utils.utcClock(new Date(agentInfo.timestamp * 1000)) +" : " +
                              agentInfo.alert.agent_update  + "</li>";
            htmlstr += "<li><b>data last updated:</b> " + agentInfo.alert.data_update  + "</li>";
            htmlstr += "<li><b>status:</b> " + agentInfo.alert.message + "</li>";
            htmlstr += "<li><b>team:</b> " + agentInfo.agent_team+ "</li>";
        };
        var detailInfo = agentInfo.down_component_detail;
        if (detailInfo && (detailInfo.length > 0)) {
            htmlstr += "<li><b>component errors:</b> ";
            htmlstr += componentFormat(detailInfo);
            htmlstr +="</li>";
           };
           
        var diskInfo = agentInfo.disk_warning;
        if (diskInfo && (diskInfo.length > 0)) {
            htmlstr += "<li><b>disk warning:</b> ";
            htmlstr += diskFullFormat(diskInfo);
            htmlstr +="</li>";
           };

        if (agentInfo.proxy_warning) {
            htmlstr += "<li><b>proxy warning:</b> " + agentInfo.proxy_warning + "</li>";
           };

        var dataError = agentInfo.data_error;
        if (dataError && dataError !== 'ok') {
            htmlstr += "<li><b>data collect error:</b> ";
            htmlstr += dataError;
            htmlstr +="</li>";
           };
        
        var couchProcess = agentInfo.couch_process_warning;
        if (couchProcess && couchProcess > 0) {
            htmlstr += "<li><b>Couch Process maxed:</b> ";
            htmlstr += couchProcess;
            htmlstr +="</li>";
           };
        
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };

    var format = function (agentData) {
       var htmlstr = "";
       var agentsWithWarning = agentData.getAlertList();
       for (i in agentsWithWarning) {
           if (agentsWithWarning[i].alert && agentsWithWarning[i].alert.status) {
               htmlstr += agentReportFormat(agentsWithWarning[i], agentsWithWarning[i].alert.status);
           }
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
