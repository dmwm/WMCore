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

WMCore.WebTools.defaultTableConfig = {
    // Set up pagination
    paginator : new YAHOO.widget.Paginator({
        rowsPerPage : 25
    }),
    // Set up initial sort state
    sortedBy: {
        key: "id", dir:YAHOO.widget.DataTable.CLASS_ASC
	}//,
	
	//initialRequest: "startIndex=0&results=25", // Initial request for first page of data 
	
    // Sorting and pagination will be routed to the server via generateRequest
    //dynamicData: true
};


WMCore.WebTools.createDataTable = function (container, dataSource, columnDefs, 
                                            tableConfig, pollingCycle) {
	 
	myColumnDefs = columnDefs

    var myDataTable = new YAHOO.widget.DataTable(container,
            myColumnDefs, dataSource, tableConfig);
    
	 // Set up polling
    var myCallback = {
         success: myDataTable.onDataReturnInitializeTable,
         failure: function() {
                YAHOO.log("Polling failure", "error");
            },
         scope: myDataTable
    };

    /*
    myDataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload) {
        oPayload.totalRecords = oResponse.meta.totalRecords;
        return oPayload;
    }
	*/	
	dataSource.setInterval(pollingCycle, null, myCallback);
	 
	return myDataTable;
};

