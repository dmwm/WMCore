WMStats.namespace("dataMap")

WMStats.dataMap.convertToColumnsAndRows = function(data, baseColumns) {
    /*
     * *
     *  convert couch db data to columns and rows.
     *  i.e. couchdb format is 
     *  {"rows":[ {"key":["A", "B", "C"],"value":{"a":0,"b":0, ...}},
     *            {"key":["D", "E", "F"],"value":{"a":1,"b":1, ...}},
     *  baseColumns is the list of column names for key value above
     *  baseColums = ["campaign", "team", "type"] then 
     *  "A" is campaign name "B" is team name, "C" is test name 
     *  
     *  this will return
     *  {'columns': ["campaign", "team", "type", "a", "b", ...],
     *   'rows': [["A", "B", "C", 0, 0, ...],
     *            ["D", "E", "F", 1, 1, ...],
     *           ]
     * */
    var columns = [];
    var rows = [];
    
    if (data.rows.length) {
        
        for (var i = 0; i < data.rows[0].key.length; i++) {
            columns.push(baseColumns[i]);
        }
         
        for (status in data.rows[0].value) {
            columns.push(status);    
        }        
    }
    
    for (var j in data.rows) {
        dataRow = [];
        for (var k in data.rows[j].key){
            dataRow.push(data.rows[j].key[k]);
        }
        for (var status in data.rows[j].value){
            dataRow.push(data.rows[j].value[status]);
        }
        rows.push(dataRow);
    }
    
    return {'columns': columns, 'rows': rows}
}


WMStats.dataMap.convertToRows = function(requestData, columnFilter) {

    if (!columnFilter) {
        // default column filter
        columnFilter = function(dataRow) {
            var row = [];
            for(var field in dataRow) {
                row.push(dataRow[field]);
            }
            return row;
        }
    }
    
    var data = requestData.getDataByRequest();
    var rows = [];
    for (var workflow in data) {
        rows.push(columnFilter(data[workflow]));       
    }
    return rows;
}
