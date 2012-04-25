WMStats.namespace("Table")

WMStats.Table = function(config) {
    var _tableConfig = {"bProcessing": true,
                        "sDom": 'C<"clear">lfrtip',
                       };
    
    function _updateConfig(config) {
        for (var prop in config) {
            _tableConfig[prop] = config[prop];
        }
        
    }
    
    /* footer is needed to use columnFilter */ 
    function _footer() {
        var footer = '<tfoot><tr>';
      
        for (var i in _tableConfig.aoColumns) {
            if (_tableConfig.aoColumns[i].bVisible != false){
                footer += '<th>' + _tableConfig.aoColumns[i]["sTitle"] + '</th>';
            }
        }
        footer += '</tr></tfoot>';
        return footer;
    }
    
    function _create(selector, filterConfig) {
        var oTable = $(selector).dataTable(_tableConfig)
        if ( oTable.length > 0 ) {
                oTable.fnAdjustColumnSizing();
        }
        if (filterConfig) {
            oTable.append(_footer());
            return oTable.columnFilter(filterConfig);
        } else {
            return oTable
        }
    }
    
    if (config) {_updateConfig(config)}
    
    return {'config': _tableConfig,
            'updateConfig': _updateConfig,
            'create': _create
            }
}
