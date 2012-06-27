WMStats.namespace("Table")

WMStats.Table = function(config) {
    var tableConfig = {
        //"sPaginationType": "full_numbers",
        "sScrollX": "100%",
        //"bScrollCollapse": true,
        "bStateSave": true,
        "bProcessing": true,
        "iDisplayLength": 10,
        "sDom": 'C<"clear">lfrtip',
        "aaSorting": []
        };
    
    function updateConfig(config) {
        for (var prop in config) {
            tableConfig[prop] = config[prop];
        }
        
    }
    
    /* footer is needed to use columnFilter */ 
    function _footer() {
        var footer = '<tfoot><tr>';
      
        for (var i in tableConfig.aoColumns) {
            if (tableConfig.aoColumns[i].bVisible != false){
                footer += '<th>' + tableConfig.aoColumns[i]["sTitle"] + '</th>';
            }
        }
        footer += '</tr></tfoot>';
        return footer;
    }
    
    function create(selector, filterConfig) {
        var oTable = $(selector).dataTable(tableConfig)
        if ( oTable.length > 0 ) {
            oTable.fnAdjustColumnSizing();
        }
        if (filterConfig) {
            //oTable.append(_footer());
            return oTable.columnFilter(filterConfig);
        } else {
            return oTable
        }
    }
    
    if (config) {updateConfig(config)}
    
    return {'config': tableConfig,
            'updateConfig': updateConfig,
            'create': create
            }
}
