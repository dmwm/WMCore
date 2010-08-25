var workflowTable = function(divID){
	var formatUrl = function(elCell, oRecord, oColumn, sData){
		elCell.innerHTML = "<a href='TaskSummaryDetail.html' target='_blank'>" + sData + "</a>";
	};
	
	var dataSchema = {
		fields: [{
			key: "wmspec",
			formatter: formatUrl
		}, {
			key: "num_task",
			label: "task"
		}, {
			key: "total_jobs",
			label: 'total'
		}, {
			key: "pending"
		}, {
			key: "processing",
			label: 'in progress'
		}, {
			key: "real_fail",
			label: "fail"
		}, {
			key: "real_success",
			label: "success"
		}]
	};
	
	var dataUrl = "/wmbs/workflowsummary"
	
	var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)
	//writeDebugObject(dataSource)
	writeEval(dataSource.responseType)
	var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, WMCore.WebTools.createDefaultTableDef(dataSchema.fields), WMCore.WebTools.createDefaultTableConfig(), 100000);
	
}