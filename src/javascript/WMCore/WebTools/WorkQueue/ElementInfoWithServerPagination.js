var elementTable = function(divID) {
        
    var formatUrl = function(elCell, oRecord, oColumn, sData) { 
            var host;
            if (!sData) {
                host = sData;
            } else {
                host = sData.split('/')[2]
            }
            elCell.innerHTML = "<a href='" + sData + "monitor' target='_blank'>" + host + "</a>"; 
        };
    
	var percentFormat = function(elCell, oRecord, oColumn, sData) { 
            if (!sData) {
                percent = 0;
            } else {
                percent = sData
            }
            elCell.innerHTML =  sData + "%"; 
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
                 //{key: "parent_flag"},
                 {key: "priority"}, {key: "num_jobs", label: "job estimation"},
                 //{key: "parent_queue_id"}, 
				 {key: "subscription_id"},
				 {key: "team_name"},
                 {key: "events_written"}, 
                 {key: "files_processed"},
                 {key: "percent_complete", label: "complete", formatter:percentFormat}, 
                 {key: "percent_success", label: "success", formatter:percentFormat},
                 {key: "insert_time", formatter:dateFormatter},
                 {key: "update_time", formatter:dateFormatter}
                ],
		metaFields: {
            totalRecords: "totalRecords"
        }
     };

    var dataUrl = "/workqueue/elementsinfowithlimit";
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)
    //overwrite default JSARRAY type to JSON
    dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;    

    var tableConfig = WMCore.WebTools.createDefaultTableConfig("id");
	tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 10});
    tableConfig.initialRequest = "?startIndex=0&results=10";
    tableConfig.dynamicData =  true;
	
    var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, 
                         WMCore.WebTools.createDefaultTableDef(dataSchema.fields),
                         tableConfig, 100000);
						 
	dataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload) { 
          oPayload.totalRecords = oResponse.meta.totalRecords; 
          return oPayload; 
    }; 
}