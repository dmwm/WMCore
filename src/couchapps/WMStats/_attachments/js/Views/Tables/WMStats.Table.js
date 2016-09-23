WMStats.namespace("Table");
WMStats.Table = function(config, tableSetting) {

    var tableSetting = tableSetting || '<table cellpadding="0" cellspacing="0" border="0.5" class="display" width="100%"></table>';
    var tableConfig = {
        //"paginationType": "full_numbers",
        //"scrollX": "100%",
        //"scrollCollapse": true,
        "stateSave": true,
        "processing": true,
        //"iDisplayLength": 10,
        "dom": '<"top"pl>rt<"bottom"ip>',
        //"sDom": 'C<"clear">lfrtip',
        "autoWidth": true,
        "jQueryUI": true	
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
                footer += '<th>' + tableConfig.aoColumns[i]["title"] + '</th>';
            }
        }
        footer += '</tr></tfoot>';
        return footer;
    }
    
    function create(selector, filterConfig) {
        $(selector).empty();
        $(selector).html(tableSetting);
        
        tableConfig.stateSaveCallback = function(settings,data) {
            localStorage.setItem(selector, JSON.stringify(data));
        };
        tableConfig.stateLoadCallback = function(settings) {
        	return JSON.parse(localStorage.getItem(selector));
        };
		
        var oTable = $(selector + " table").DataTable(tableConfig);
        if ( oTable.length > 0 ) {
            oTable.columns.adjust().draw();
        }
        
        jQuery(WMStats.Globals.Event).triggerHandler(WMStats.CustomEvents.LOADING_DIV_END);
        
        //TODO: enable column filter
        //if (filterConfig) {
           //oTable.append(_footer());
           //https://datatables.net/reference/api/column().search()
        //}
        return oTable;
    }
    
    if (config) {updateConfig(config);}
    
    return {'config': tableConfig,
            'updateConfig': updateConfig,
            'create': create
           };
};
