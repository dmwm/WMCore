WMStats.namespace("Table")

WMStats.Table = function(config) {
    var _defaultConfig = {"bProcessing": true,
                          "sDom": 'C<"clear">lfrtip',
                          };
    
    function _updateConfig(config) {
        for (var prop in config) {
            _defaultConfig[prop] = config[prop];
        }
    }
    
    function _create(selector) {
        return $(selector).dataTable(_defaultConfig).columnFilter();
    }
    
    if (config) {_updateConfig(config)}
    
    return {'config': _defaultConfig,
            'updateConfig': _updateConfig,
            'create': _create
            }
}
