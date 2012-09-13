WMStats.namespace("Agents");

WMStats.Agents = function (couchData) {
    
    var agentData = new WMStats._StructBase();
    
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
    
    return agentData
}
