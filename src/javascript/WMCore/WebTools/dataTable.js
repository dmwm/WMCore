WMCore.WebTools.dataSchema = function () {
	
	var formatUrl = function(elCell, oRecord, oColumn, sData) { 
	            elCell.innerHTML = "<a href='" + oRecord.getData("ChildQueueUrl") + "' target='_blank'>" + sData + "</a>"; 
	};
	
	var dataSchema =  {
        fields: [{key: "Status", sortable:true}, {key: "ChildQueueUrl", label: "Child Queue Url", sortable:true, formatter:formatUrl}]
    };
}

WMCore.WebTools.createDataSource = function (dataUrl, dataSchema) {
	
	//var myDataSource = new YAHOO.util.DataSource(dataUrl);
	//myDataSource.connXhrMode = "queueRequests"; 
	var myDataSource = new YAHOO.util.XHRDataSource(dataUrl);
	//var myDataSource = YAHOO.util.ScriptNodeDataSource(dataUrl);
    myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
    myDataSource.responseSchema = dataSchema
	
	return myDataSource
}

WMCore.WebTools.createDefaultTableDef = function (tableDef) {
	var defaultDef = new Array();
	for (var i in tableDef) {
		defaultDef[i] = YAHOO.lang.merge(tableDef[i], {
			sortable: true
			//resizeable: true
		})
	}
    return defaultDef
}

WMCore.WebTools.defaultTableConfig = {
    // Set up pagination
    paginator : new YAHOO.widget.Paginator({
        rowsPerPage : 50
    }),
    // Set up initial sort state
    sortedBy: {
        key: "id"
	}
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
    }
	
	dataSource.setInterval(pollingCycle, null, myCallback)
	 
	return myDataTable
}

WMCore.WebTools.createChart = function (dataSource, container, pollingCycle) {
	
	YAHOO.widget.Chart.SWFURL = "http://yui.yahooapis.com/2.8.0r4/build/charts/assets/charts.swf";
	
	var yAxis = new YAHOO.widget.NumericAxis();
	yAxis.minimum = 0;
	yAxis.maximum = 100;

	var mychart = new YAHOO.widget.ColumnChart( "chart", dataSource, dataSource.responseShema);
	return mychart
}

YAHOO.util.Connect.initHeader("Accept", "text/json", true)