WMCore.namespace("WebTools.WMBS.WorkflowSummaryWithTask")

WMCore.WebTools.WMBS.WorkflowSummaryWithTask.taskTable = function(oArgs){

    var dataSchema = {
        fields: [{key: "task"},
                 {key: "total_jobs", label: 'total'},
                 {key: "none"}, {key: "new"},
                 {key: "created"}, {key: "createfailed"},{key: "createcooloff"},
                 {key: "submitfailed"}, {key: "submitcooloff"},
                 {key: "executing"}, {key: "complete"},
                 {key: "jobfailed"}, {key: "jobcooloff"},
                 {key: "real_fail", label: "fail"},
                 {key: "real_success", label: "success"}]
        };
    
    var dataUrl = "/wmbs/tasksummary/" + oArgs.workflow
    
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.WebTools.createDataTable(oArgs.divID, dataSource, 
                               WMCore.WebTools.createDefaultTableDef(dataSchema.fields), 
                               WMCore.WebTools.createDefaultTableConfig(), 100000, true)
	var callback = {
		success: function(){
			//workflowTable.onDataReturnInitializeTable();
			//dataTable.onDataReturnInitializeTable();
		},
		failure: function(){
			YAHOO.log("Polling failure", "error");
		},
		scope: this
	};
	dataSource.sendRequest(null, callback);
};

var workflowTable = function(oArgs){
    var postfixLink = "/wmbsmonitor/template/TaskSummary?workflow=";
    
    var formatUrl = function(elCell, oRecord, oColumn, sData){
        if (!oArgs.firstWorkflow) {
            oArgs.firstWorkflow = sData
        };
        elCell.innerHTML = "<a href='" + postfixLink + sData + "' target='_blank'>" + sData + "</a>";
    };
    
    var dataSchema = {
        fields: [{key: "wmspec", formatter: formatUrl},
                 {key: "total_jobs", label: 'total'},
                 {key: "pending"},
                 {key: "processing", label: 'in progress'},
                 {key: "real_fail", label: "fail"},
                 {key: "real_success", label: "success"}]
    };
    var dataUrl = "/wmbs/workflowsummary";
    
    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema);
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
			
    var dataTable = WMCore.WebTools.createDataTable(oArgs.divID, dataSource, 
                               WMCore.WebTools.createDefaultTableDef(dataSchema.fields), 
                               WMCore.WebTools.createDefaultTableConfig(), 100000, true);
    
	var callback = {
        success: function (){
			var args = {};
            args.workflow = oArgs.firstWorkflow;
            args.divID = oArgs.taskDivID;
            taskTable(args);
			dataTable.onDataReturnInitializeTable();
			},
        failure: function(){
                YAHOO.log("Polling failure", "error");
            },
        scope: this
    };
	
    dataSource.setInterval(10000, null, callback);
    
	dataTable.subscribe("cellClickEvent", oArgs.callBack);
};