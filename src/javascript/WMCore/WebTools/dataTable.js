/*
 * Provide default values for datasource and table.
 * 
 */

WMCore.createDataSource = function (dataUrl, dataSchema, type) {
	
	if (type == "Local") {
	   var myDataSource = new YAHOO.util.LocalDataSource(dataUrl);
	} else {
		var myDataSource = new YAHOO.util.XHRDataSource(dataUrl);
	};
	
	//var myDataSource = YAHOO.util.ScriptNodeDataSource(dataUrl);
    myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    myDataSource.responseSchema = dataSchema;

    //Always send accept type as text/json
    YAHOO.util.Connect.initHeader("Accept", "text/json", true);
    
    return myDataSource;
};

WMCore.createDefaultTableDef = function (tableDef) {
    var defaultDef = new Array();
    for (var i in tableDef) {
        defaultDef[i] = YAHOO.lang.merge(tableDef[i], {
            sortable: true,
            resizeable: true
        });
    }
    return defaultDef;
};

WMCore.createDefaultTableConfig = function(sortBy) {
    
    var defaultConfig = {};
    // Set up pagination
    defaultConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 25});
    // Set up pagination
    if (sortBy) {
       defaultConfig.sortedBy = {key: sortBy, dir:YAHOO.widget.DataTable.CLASS_ASC};
    };

    return defaultConfig
};


WMCore.createDataTable = function (container, dataSource, columnDefs, 
                                            tableConfig, pollingCycle, myCallback) {
	 
    var myDataTable = new YAHOO.widget.DataTable(container,
            columnDefs, dataSource, tableConfig);
    
    if (myCallback === undefined) {
        // Set up polling
        myCallback = {
            success: myDataTable.onDataReturnReplaceRows,
            failure: function(){
                YAHOO.log("Polling failure", "error");
            },
            scope: myDataTable,
            arguments: myDataTable.getState()
        };
        dataSource.setInterval(pollingCycle, null, myCallback);
    };
    
    return myDataTable;
};

