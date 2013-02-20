WMStats.namespace("Table")

WMStats.Table = function(config, tableSetting) {

    var tableSetting = tableSetting || '<table cellpadding="0" cellspacing="0" border="0.5" class="display" width="100%"></table>';
    var tableConfig = {
        //"sPaginationType": "full_numbers",
        //"sScrollX": "100%",
        //"bScrollCollapse": true,
        "bStateSave": true,
        "bProcessing": true,
        //"iDisplayLength": 10,
        "sDom": '<"top"pl>rt<"bottom"ip>',
        //"sDom": 'C<"clear">lfrtip',
        "aaSorting": [],
        "bAutoWidth": true,
        "bJQueryUI": true
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
        $(selector).empty();
        $(selector).html(tableSetting);
        var oTable = $(selector + " table").dataTable(tableConfig)
        if ( oTable.length > 0 ) {
            oTable.fnAdjustColumnSizing();
        }
        
        jQuery(WMStats.Globals.Event).triggerHandler(WMStats.CustomEvents.LOADING_DIV_END);
        
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
};
