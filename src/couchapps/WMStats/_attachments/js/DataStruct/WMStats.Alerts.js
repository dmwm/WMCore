WMStats.namespace("Alerts");

WMStats.Alerts = function (couchData) {
    
    var alertData = new WMStats._StructBase();
    
    alertData.convertCouchData =   function(data) {
                                        var dataRows = data.rows;
                                        var rows =[];
                                        for (var i in dataRows) {
                                            var tableRow = {};
                                            tableRow.workflow = dataRows[i].key;
                                            tableRow.count = dataRows[i].value;
                                            rows.push(tableRow);
                                        }
                                        return rows;
                                   };
    if (couchData) alertData.setData(couchData);
    
    return alertData;
};
