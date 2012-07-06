WMStats.namespace("Sites");

WMStats.Sites = function (couchData) {
    var _data;
    var baseColumns = ["timestamp", "site", "agent_url"];
    
    var setData = function(data) {
        var dataRows = data.rows
        var rows =[]
        for (var i in dataRows) {
            var tableRow = dataRows[i].value;
            for (var j = 0; j < baseColumns.length; j ++) {
                tableRow[baseColumns[j]] = dataRows[i].key[j];
            }
            rows.push(tableRow);
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
