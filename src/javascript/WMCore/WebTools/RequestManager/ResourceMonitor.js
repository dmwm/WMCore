WMCore.namespace("WebTools.RequestManager.ResourceMonitor");

WMCore.WebTools.RequestManager.ResourceMonitor.resourceInfo = function(divID){
	var dataSchema = {
		fields: [{
			key: "site"
		}, {
			key: "total_slots"
		}, {
			key: "running_jobs",  label: "jobs in wmagents"
		}]
	};
	
	var dataUrl = "/reqMgr/resourceInfo"
	
	var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema);
	var dataTable = WMCore.WebTools.createDataTable(divID, 
	                dataSource, WMCore.WebTools.createDefaultTableDef(dataSchema.fields), 
					WMCore.WebTools.createDefaultTableConfig('running_jobs'), 100000);
}