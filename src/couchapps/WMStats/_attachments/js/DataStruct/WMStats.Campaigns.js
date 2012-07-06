WMStats.namespace("Campaigns");

WMStats.Campaigns = function (couchData) {
    
    var _data;
    var _baseColumns = ["campaign"];
        
    function setData(data) {
        dataRows = data.rows
         //TODO sync with group level
        //This is base on the group_level ["campaign", "team", type]
        var rows =[]
        for (var i in dataRows) {
            var tableRow = dataRows[i].value;
            for (var j = 0; j < _baseColumns.length; j ++) {
                tableRow[_baseColumns[j]] = dataRows[i].key[j];
            }
            rows.push(tableRow)
        }
        _data = rows;
    }
    
    function getData() {
        return _data;
    }
    
    if (couchData) {
        setData(couchData);
    }
    
    return {
        getData: getData,
        setData: setData
    }
}
