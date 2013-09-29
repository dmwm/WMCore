WMStats.namespace("Sites");

WMStats.Sites = function (couchData) {
    var _data;
    var baseColumns = ["timestamp", "site", "agent_url"];
    
    var siteData = new WMStats._StructBase();
    
    siteData.convertCouchData = function(data) {
                                    var dataRows = data.rows;
                                    var rows =[];
                                    for (var i in dataRows) {
                                        var tableRow = dataRows[i].value;
                                        for (var j = 0; j < baseColumns.length; j ++) {
                                            tableRow[baseColumns[j]] = dataRows[i].key[j];
                                        }
                                        rows.push(tableRow);
                                    }
                                    return rows;
                               };
    
    if (couchData) siteData.setData(couchData);
    
    return siteData;
};
