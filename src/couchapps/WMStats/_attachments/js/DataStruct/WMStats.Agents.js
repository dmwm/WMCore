WMStats.namespace("Agents");

WMStats.Agents = function (couchData) {
    
    var agentData = new WMStats._StructBase();
    agentData.agentNumber = {error: 0, stable:0}
    
    agentData.convertCouchData = function(data) {
                                     var dataRows = data.rows;
                                     var rows = [];
                                     for (var i in dataRows) {
                                         var tableRow = dataRows[i].value;
                                         rows.push(tableRow)
                                     }
                                     return rows;
                                 }
    if (couchData) agentData.setData(couchData);
    
    agentData.getAlertList = function() {
        var currentTime = Math.round(new Date().getTime() / 1000);
        var dataList = this.getData();
        var agentPollingCycle = 600;
        agentNumber = {error: 0, stable:0};
        
        function getStatus(agentInfo) {
            var lastUpdatedDuration = currentTime - agentInfo.timestamp;
            if (lastUpdatedDuration > agentPollingCycle * 2) {
                agentData.agentNumber.error += 1;
                return {status: "agent_down", 
                        message: WMStats.Utils.foramtDuration(lastUpdatedDuration)};
            } else if (agentInfo.down_components.length > 0) {
                agentData.agentNumber.error += 1;
                return {status: "component_down",
                        message: agentInfo.down_components};
            } else {
                agentData.agentNumber.stable += 1;
                return {status: "ok", 
                        message: WMStats.Utils.foramtDuration(lastUpdatedDuration)};
            } 
        };
        
        for (var index in dataList) {
            dataList[index]['alert'] = getStatus(dataList[index]);
        }
        return dataList;
    }
    return agentData
};
