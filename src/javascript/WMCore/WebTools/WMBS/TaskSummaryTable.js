WMCore.namespace("WebTools.WMBS.TaskSummaryTable")

WMCore.WebTools.WMBS.TaskSummaryTable.taskTable = function(oArgs){

	var dataSchema = {
        fields: [{key: "task", label: " Tasks for " + oArgs.workflow},
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
							   WMCore.WebTools.createDefaultTableConfig(), 1000000)
}