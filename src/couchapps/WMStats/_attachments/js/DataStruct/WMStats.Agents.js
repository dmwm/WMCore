WMStats.namespace("Agents");

WMStats.Agents = function (couchData) {
    
    var agentData = new WMStats._StructBase();
    agentData.agentNumber = {error: 0, stable:0};
    
    agentData.convertCouchData = function(data) {
                                     var dataRows = data.rows;
                                     var rows = [];
                                     for (var i in dataRows) {
                                         var tableRow = dataRows[i].value;
                                         rows.push(tableRow);
                                     };
                                     return rows;
                                };
    if (couchData) agentData.setData(couchData);
    
    agentData.getAlertList = function(){
        var currentTime = Math.round(new Date().getTime() / 1000);
        var dataList = this.getData();
        var agentPollingCycle = 600;
        agentData.agentNumber = {error: 0, warning:0, stable:0, drain: 0};
        
        function getStatus(agentInfo) {
            var lastUpdatedDuration = currentTime - agentInfo.timestamp;
            var dataUpdateDuration = -1; 
            if (agentInfo.data_last_update) {
                var dataUpdateDuration = currentTime - agentInfo.data_last_update; 
            }
            var report = {"status": "stable", "message": ""};
            
            // drain case            
            if (agentInfo.drain_mode) {
                report.status = "drain";
                report.message += "Draining Agent; ";
            }

            if (agentInfo.proxy_warning && agentInfo.status === "warning") {
                report.status = agentInfo.status;
                report.message += "Proxy expiration warning; ";
            } else if (agentInfo.proxy_warning && agentInfo.status === "error") {
                agentData.agentNumber.error += 1;
                report.status = agentInfo.status;
                report.message += "Proxy expiration error; ";
            }
            
            // warning case
            if (agentInfo.couch_process_warning) {
                agentData.agentNumber.error += 1;
                report.status = "error";
                report.message += "couchdb process maxed out: " + agentInfo.couch_process_warning;
            }
            
            if (agentInfo.disk_warning && (agentInfo.disk_warning.length > 0)) {
                agentData.agentNumber.warning += 1;
                report.status = "warning";
                report.message += "disk is almost full; ";
            }

            if (lastUpdatedDuration > agentPollingCycle * 2) {
                agentData.agentNumber.error += 1;
                report.status = "error"; 
                report.message += "Agent Data is not updated: AgentStatusWatcher is Down; ";
            }
            
            if (agentInfo.down_components.length > 0) {
                agentData.agentNumber.error += 1;
                report.status = "error";
                report.message += "Components or Thread down; ";
            }
            
            if (agentInfo.data_error && (agentInfo.data_error !== "ok")) {
                agentData.agentNumber.error += 1;
                report.status = "error";
                report.message += "Data collect error; ";
            }
            // If there is no drain/warning or errors set the message OK
            if (report.status === "stable") {
                agentData.agentNumber.stable += 1;
                report.message = "OK";
            };
            
            report["agent_update"] =  WMStats.Utils.formatDuration(lastUpdatedDuration);
            report["data_update"] = WMStats.Utils.formatDuration(dataUpdateDuration);
            return report;
        };
        
        for (var index in dataList) {
            dataList[index]['alert'] = getStatus(dataList[index]);
        };
        return dataList;
    };
    // initial calculation
    agentData.getAlertList();
    
    return agentData;
};
