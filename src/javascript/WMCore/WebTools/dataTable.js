/*
 * Provide default values for datasource and table.
 * 
 */

WMCore.WebTools.createDataSource = function (dataUrl, dataSchema) {
	
	//var myDataSource = new YAHOO.util.DataSource(dataUrl);
	//myDataSource.connXhrMode = "queueRequests"; 
	var myDataSource = new YAHOO.util.XHRDataSource(dataUrl);
	//var myDataSource = YAHOO.util.ScriptNodeDataSource(dataUrl);
    myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    myDataSource.responseSchema = dataSchema;
	
	//Always send accept type as text/json
	YAHOO.util.Connect.initHeader("Accept", "text/json", true)
	
	return myDataSource;
};

WMCore.WebTools.createDefaultTableDef = function (tableDef) {
	var defaultDef = new Array();
	for (var i in tableDef) {
		defaultDef[i] = YAHOO.lang.merge(tableDef[i], {
			sortable: true,
			resizeable: true
		});
	}
    return defaultDef;
};

WMCore.WebTools.createDefaultTableConfig = function() {
	
	var defaultConfig = {
	    // Set up pagination
	    paginator : new YAHOO.widget.Paginator({
	        rowsPerPage : 25
	    }),
	    // Set up initial sort state
	    sortedBy: {
	        key: "id", dir:YAHOO.widget.DataTable.CLASS_ASC
		}
    }
	return defaultConfig
};


WMCore.WebTools.createDataTable = function (container, dataSource, columnDefs, 
                                            tableConfig, pollingCycle) {
	 
    var myDataTable = new YAHOO.widget.DataTable(container,
            columnDefs, dataSource, tableConfig);
    
	 // Set up polling
    var myCallback = {
         success: myDataTable.onDataReturnInitializeTable,
         failure: function() {
                YAHOO.log("Polling failure", "error");
            },
         scope: myDataTable
    };

    dataSource.setInterval(pollingCycle, null, myCallback);
	 
	return myDataTable;
};

