WMCore.namespace("WebTools.RequestManager.Overview")

WMCore.WebTools.RequestManager.Overview.overviewTable = function(divID, filterDiv, filterFunction){
    
	var postfixLink = "/template/ElementSummaryByWorkflow?workflow=";
	
	var formatRequest = function(elCell, oRecord, oColumn, sData) { 
            elCell.innerHTML = "<a href='../reqMgrBrowser/requestDetails/" + sData  +
			                            "' target='_blank'>" + sData + "</a>";
        };
    
	var formatGlobalQ = function(elCell, oRecord, oColumn, sData) { 
            var host;
            if (!sData) {
                elCell.innerHTML = "Not Assigned";
            } else {
                host = sData.split('/')[2];
				elCell.innerHTML = "<a href='" + sData  + "monitor" + postfixLink + oRecord.getData("request_name") + "' target='_blank'>" + host + "</a>";
            };
        };
	
	var formatLocalQ = function(elCell, oRecord, oColumn, sData) { 
            var host;
            if (!sData || ! sData.length) {
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
			if (oRecord.getData("error")) {
                 elCell.innerHTML = "<font color='red'> " + oRecord.getData("error") + "<font>";
                 return;
            };
            localQueueList  = oRecord.getData("local_queue")
			if (!localQueueList || !localQueueList.length) {
			     elCell.innerHTML = "Not Assigned";
				 return;
			};
            if (!sData) {
                if (oRecord.getData("couch_error")) {
                    elCell.innerHTML = "<font color='red'> Can't connect CouchDB <font>";
                } else {
                    elCell.innerHTML = "No jobs in CouchDB";
                }
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
	
	var pbs = [];
    var progressFormatter = function (elLiner, oRecord, oColumn, oData) {
		  var total = (oRecord.getData("pending") + oRecord.getData("pending")
		               + oRecord.getData("running") + oRecord.getData("cooloff")
					   + oRecord.getData("success") + oRecord.getData("failure"));
					   
		  var completed = oRecord.getData("success") + oRecord.getData("failure");
		  
		  //if total is 0 make 0% complete
		  if (total == 0) {
		  	  total = 1;
		  };
          var pb = new YAHOO.widget.ProgressBar({
                     width:'90px',
                    height:'11px',
                    maxValue: total,
                    //className:'some_other_image',
                    value:completed
           }).render(elLiner);
                pbs.push(pb);
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
				 {key: "couch_job_info_base"},
				 {key: "host"},
				 {key: "couch_error"},
				 {key: "error"}
                 ]
        };

   var dataTableCols = [{key: "request_name", formatter:formatRequest},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue", formatter:formatGlobalQ},
                 {key: "local_queue", formatter:formatLocalQ},
                 {key: "pending", formatter:formatPending},
                 {key: "cooloff", formatter:formatCoolOff},
                 {key: "running", formatter:formatRunning},
                 {key: "success", formatter:formatSuccess},
                 {key: "failure", formatter:formatFailure},
                 {key: "couch_doc_base", label: "summary", formatter:formatCouchDB},
				 {key: "progress", formatter:progressFormatter}
                 ];
		    
    var dataUrl = "/reqMgr/overview"
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)

    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, WMCore.WebTools.createDefaultTableDef(dataTableCols), WMCore.WebTools.createDefaultTableConfig(), 100000)
}
