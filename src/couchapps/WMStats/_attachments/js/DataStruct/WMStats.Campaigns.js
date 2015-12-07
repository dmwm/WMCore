WMStats.namespace("Campaigns");

WMStats.Campaigns = function (couchData) {
    
   var campaignData = new WMStats._StructBase();
   var _baseColumns = ["campaign"];
    
   campaignData.convertCouchData =  function(data) {
                                        dataRows = data.rows;
                                         //TODO sync with group level
                                        //This is base on the group_level ["campaign", "team", type]
                                        var rows =[];
                                        for (var i in dataRows) {
                                            var tableRow = dataRows[i].value;
                                            for (var j = 0; j < _baseColumns.length; j ++) {
                                                tableRow[_baseColumns[j]] = dataRows[i].key[j];
                                            }
                                            rows.push(tableRow);
                                        }
                                        return rows;
                                   };
    if (couchData) campaignData.setData(couchData);
    
    return campaignData;
};
