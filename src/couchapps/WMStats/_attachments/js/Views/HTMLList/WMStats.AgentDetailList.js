WMStats.namespace("AgentDetailList");

(function () {
    
    var componentFormat = function(componentList) {
        var formatStr = "";
        for (var i in componentList) {
            formatStr += "<details> <summary>" + componentList[i].name +"</summary> <ul>";
            formatStr += "<li><b>worker thread:</b> " + componentList[i].worker_name +"</li>";
            formatStr += "<li><b>status:</b> " + componentList[i].state + "</li>";
            //formatStr += "<li><b>error:</b> " + WMStats.Utils.utcClock(new Date(componentList[i].last_error * 1000)) + "</li>";
            formatStr += "<li><b>last updated:</b> " + WMStats.Utils.utcClock(new Date(componentList[i].last_updated * 1000)) + "</li>";
            formatStr += "<li><b>pid:</b> " + componentList[i].pid + "</li>";
            formatStr += "<li><b>error message:</b> <pre>" + componentList[i].error_message + "</pre></li>";
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
            if (agentInfo.agent_team) {
                htmlstr += "<li><b>team:</b> " + agentInfo.agent_team+ "</li>";
            }
        };
        var componentsDown = agentInfo.down_components;
        if (componentsDown && (componentsDown.length > 0)) {
            htmlstr += "<li><b>component errors for:</b> " + componentsDown;
            if (agentInfo.down_component_detail && (agentInfo.down_component_detail.length > 0)) {
                htmlstr += componentFormat(agentInfo.down_component_detail);
            }
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

        if (agentInfo.drain_stats) {
            htmlstr += "<li><b>drain statistics:</b></li><ul>";
            htmlstr += "<li>workflows completed (" + agentInfo.drain_stats.workflows_completed + ")</li>";
            if (agentInfo.drain_stats.workflows_completed) {
                htmlstr += "<li>condor running (" + agentInfo.drain_stats.condor_status.running + ")</li>";
                htmlstr += "<li>condor idle (" + agentInfo.drain_stats.condor_status.idle + ")</li>";
                htmlstr += "<li>dbs open blocks (" + agentInfo.drain_stats.upload_status.dbs_open_blocks + ")</li>";
                htmlstr += "<li>dbs not uploaded (" + agentInfo.drain_stats.upload_status.dbs_notuploaded + ")</li>";
                htmlstr += "<li>phedex not uploaded (" + agentInfo.drain_stats.upload_status.phedex_notuploaded + ")</li>";
                for (var key in agentInfo.drain_stats.global_wq_status) {
                    htmlstr += "<li>global WQ in '" + key + "' status (" + agentInfo.drain_stats.global_wq_status[key] + ")</li>";
                }
                for (var key in agentInfo.drain_stats.local_wq_status) {
                    htmlstr += "<li>local WQ in '" + key + "' status (" + agentInfo.drain_stats.local_wq_status[key] + ")</li>";
                }
                for (var key in agentInfo.drain_stats.local_wqinbox_status) {
                    htmlstr += "<li>local WQInbox in '" + key + "' status (" + agentInfo.drain_stats.local_wqinbox_status[key] + ")</li>";
                }
                // only print job information that are != than 0
                htmlstr += "<li>there are no wmbs jobs in the agent, except for...</li>";
                for (var key in agentInfo.WMBS_INFO.wmbsCountByState) {
                    if (agentInfo.WMBS_INFO.wmbsCountByState[key]) {
                        htmlstr += "<li>  jobs in '" + key + "' state (" + agentInfo.WMBS_INFO.wmbsCountByState[key] + ")</li>";
                    }
                }
            }
            htmlstr += "</ul>"
        }


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
