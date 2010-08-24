var elementTable = function(divID) {
        
    var formatUrl = function(elCell, oRecord, oColumn, sData) {
		    var host;
            if (!sData) {
                host = sData;
            } else {
                host = sData.split('/')[2]
            } 
            elCell.innerHTML = "<a href='" + sData + "monitor' target='_blank'>" + host + " </a>"; 
        };
        
    var dateFormatter = function(elCell, oRecord, oColumn, oData) {
        
        var oDate = new Date(oData*1000);
        //for the formatting check 
        // http://developer.yahoo.com/yui/docs/YAHOO.util.Date.html#method_format
        var str = YAHOO.util.Date.format(oDate, { format:"%D %T"});
        elCell.innerHTML = str;
    };
    
    var dataSchema = {
        resultsList: "data",
        fields: [{key: "id"}, {key: "spec_name"}, {key: "task_name"}, {key: "element_name"}, 
                 {key: "status"}, {key: "child_queue", formatter:formatUrl}, 
                 {key: "parent_flag"},
                 {key: "priority"}, {key: "num_jobs"},
                 {key: "parent_queue_id"}, {key: "subscription_id"},
                 {key: "insert_time", formatter:dateFormatter},
                 {key: "update_time", formatter:dateFormatter}
                ],
        metaFields: {
            totalRecords: "totalRecords"
        }
     };

    var dataUrl = "/workqueue/elementsinfowithlimit?";
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)
    //overwrite default JSARRAY type to JSON
    dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;    

    var tableConfig = WMCore.WebTools.createDefaultTableConfig("id");
    tableConfig.paginator = new YAHOO.widget.Paginator({
        rowsPerPage: 10, // REQUIRED
        //totalRecords: myData.length, // OPTIONAL
 
        // use an existing container element
        //containers: 'my_pagination',
 
        // use a custom layout for pagination controls
        template: "{PageLinks} Show {RowsPerPageDropdown} per page",
 
        // show all links
        pageLinks: YAHOO.widget.Paginator.VALUE_UNLIMITED,
 
        // use these in the rows-per-page dropdown
        rowsPerPageOptions: [10, 25, 50],
 
        // use custom page link labels
        pageLabelBuilder: function (page, paginator) {
            var recs = paginator.getPageRecords(page);
            return (recs[0] + 1) + ' - ' + (recs[1] + 1);
        }
    })
	
    tableConfig.initialRequest = "startIndex=0&results=10";
    tableConfig.dynamicData =  true;
    
    var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, 
                         WMCore.WebTools.createDefaultTableDef(dataSchema.fields),
                         tableConfig, 100000);
                        
    dataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload) { 
          oPayload.totalRecords = oResponse.meta.totalRecords; 
          return oPayload; 
    }; 
}