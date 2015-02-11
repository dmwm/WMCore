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
        agentData.agentNumber = {error: 0, warning:0, stable:0};
        
        function getStatus(agentInfo) {
            var lastUpdatedDuration = currentTime - agentInfo.timestamp;
            var dataUpdateDuration = -1; 
            if (agentInfo.data_last_update) {
            	var dataUpdateDuration = currentTime - agentInfo.data_last_update; 
            }
            var report = {};
            if (lastUpdatedDuration > agentPollingCycle * 2) {
                agentData.agentNumber.error += 1;
                report = {status: "error", 
                          message: "Data is not updated: AnalyticsDataCollector Down"};
            } else if (agentInfo.down_components.length > 0) {
				agentData.agentNumber.error += 1;
                report = {status: "error",
                          message:"Components or Thread down"};
            } else if (agentInfo.data_error && (agentInfo.data_error !== "ok")) {
            	agentData.agentNumber.error += 1;
            	report = {status: "error",
                          message: "Data collect error"};
            } else if (agentInfo.couch_process_warning) {
            	agentData.agentNumber.error += 1;
            	report = {status: "error",
                          message: "couchdb process maxed out: " + agentInfo.couch_process_warning};
            } else if (agentInfo.drain_mode) {
                agentData.agentNumber.warning += 1;
                report = {status: "warning",
                          message: "Draining Agent"};
            } else if (agentInfo.disk_warning && (agentInfo.disk_warning.length > 0)) {
            	agentData.agentNumber.warning += 1;
            	report = {status: "warning",
                          message: "disk is almost full" };
            } else {
                agentData.agentNumber.stable += 1;
                report = {status: "ok", 
                          message: "OK"};
            };
            
            report["agent_update"] =  WMStats.Utils.foramtDuration(lastUpdatedDuration);
            report["data_update"] = WMStats.Utils.foramtDuration(dataUpdateDuration);
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
