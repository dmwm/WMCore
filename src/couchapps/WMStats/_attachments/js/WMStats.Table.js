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
    
    function _addFooter(selector) {
        var footer = '<tfoot><tr>';
      
        for (var i in _tableConfig.aoColumns) {
            if (_tableConfig.aoColumns[i].bVisible != false){
                footer += '<th>' + _tableConfig.aoColumns[i]["sTitle"] + '</th>';
            }
        }
        footer += '</tr></tfoot>';
        $(selector).append(footer)
    }
    
    function _create(selector) {
        oTable = $(selector).dataTable(_tableConfig)
        _addFooter(selector);
        return oTable.columnFilter();
    }
    
    if (config) {_updateConfig(config)}
    
    return {'config': _tableConfig,
            'updateConfig': _updateConfig,
            'create': _create
            }
}
