var overviewTable = function(divID){
    
	var postfixLink = "/template/ElementSummaryByWorkflow?workflow=";
	
	var formatGlobalQ = function(elCell, oRecord, oColumn, sData) { 
            var host;
            if (!sData) {
                elCell.innerHTML = "Not Assigned";
            } else {
                host = sData.split('/')[2];
				elCell.innerHTML = "<a href='" + sData + postfixLink + oRecord.getData("request_name") + "' target='_blank'>" + host + "</a>"; 
            };
        };
	
	var formatLocalQ = function(elCell, oRecord, oColumn, sData) { 
            var host;
            if (!sData) {
                elCell.innerHTML = "Not Assigned";
            } else {
				for (data in sData) {
					host = sData[data].split('/')[2];
					elCell.innerHTML = "<a href='" + sData + "monitor" + postfixLink + oRecord.getData("request_name") + "' target='_blank'>" + host + "</a> <br>";
				};
			}; 
        };
    
	var formatCouchDB = function(elCell, oRecord, oColumn, sData) { 
            var host;
			if (!oRecord.getData("local_queue")) {
			     elCell.innerHTML = "Not yet Assigned";
				 return	
			}
            if (!sData) {
                elCell.innerHTML = "<font color='red'> Can't connect CouchDB <font>";
            } else {
                host = "CouchDB Link";
				elCell.innerHTML = "<a href='" + sData + "' target='_blank'>" + host + "</a>";
            };
        };
		
	var formatJobLink = function(elCell, oRecord, oColumn, sData, type) { 
            var host;
            if (!sData) {
                elCell.innerHTML = 0;
            } else {
                host = sData;
                elCell.innerHTML = "<a href='" + oRecord.getData("couch_job_info_base").replace("replace_to_", type) + "' target='_blank'>" + host + "</a>";
            };
        };
	var formatPending = function(elCell, oRecord, oColumn, sData) { 
            formatJobLink(elCell, oRecord, oColumn, sData, "pending")
    };
	
	var formatRunning = function(elCell, oRecord, oColumn, sData) { 
            formatJobLink(elCell, oRecord, oColumn, sData, "running")
    };
	
	var formatCoolOff = function(elCell, oRecord, oColumn, sData) { 
            formatJobLink(elCell, oRecord, oColumn, sData, "cooloff")
    };
	
	var formatSuccess = function(elCell, oRecord, oColumn, sData) { 
            formatJobLink(elCell, oRecord, oColumn, sData, "success")
    };
	
	var formatFailure = function(elCell, oRecord, oColumn, sData) { 
            formatJobLink(elCell, oRecord, oColumn, sData, "failed")
    };
	
    var dataSchema = {
        fields: [{key: "request_name"},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue"},
                 {key: "local_queue"},
                 {key: "pending"},
                 {key: "cooloff"},
                 {key: "running"},
                 {key: "success"},
                 {key: "failure"},
                 {key: "couch_doc_base"},
				 {key: "couch_job_info_base"}]
        };

        var dataTableCols = [{key: "request_name"},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue", formatter:formatGlobalQ},
                 {key: "local_queue", formatter:formatLocalQ},
                 {key: "pending", formatter:formatPending},
                 {key: "cooloff", formatter:formatCoolOff},
                 {key: "running", formatter:formatRunning},
                 {key: "success", formatter:formatSuccess},
                 {key: "failure", formatter:formatFailure},
                 {key: "couch_doc_base", label: "summary", formatter:formatCouchDB}
                 ];
         
    var dataUrl = "/reqMgr/overview"
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, WMCore.WebTools.createDefaultTableDef(dataTableCols), WMCore.WebTools.createDefaultTableConfig(), 100000)
}