WMStats.namespace("Agents");

WMStats.Agents = function () {
    var _data;
    
    function setData(data) {
        var dataRows = data.rows;
        var rows = [];
        for (var i in dataRows) {
            var tableRow = dataRows[i].value;
            rows.push(tableRow)
        }
        _data = rows;
        return rows;
    }
    
    function getData() {
        return _data;
    }
    
    return {
        getData: getData,
        setData: setData
    }
}()
