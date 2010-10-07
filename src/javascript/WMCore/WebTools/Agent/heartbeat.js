WMCore.namespace("WebTools.Agent.heartbeat")

WMCore.WebTools.Agent.heartbeat.heartbeatTable = function(divID){
	var dateFormatter = function(elCell, oRecord, oColumn, oData){
	
		var oDate = new Date(oData * 1000);
		//for the formatting check 
		// http://developer.yahoo.com/yui/docs/YAHOO.util.Date.html#method_format
		var str = YAHOO.util.Date.format(oDate, {
			format: "%M:%S"
		});
		elCell.innerHTML = str;
	}
	
	var colorRow = function(elTr, oRecord){
		if (oRecord.getData('alarm') < 0) {
			YAHOO.util.Dom.addClass(elTr, 'mark');
		}
		else 
			if (oRecord.getData('last_error') > 0) {
				YAHOO.util.Dom.addClass(elTr, 'warning');
			};
		return true;
	};
	
	var dataSchema = {
		fields: [{
			key: "name",
			label: "component"
		}, {
			key: 'pid'
		}, {
			key: "worker_name",
			label: "worker"
		}, {
			key: "ago",
			label: "last updated",
			formatter: dateFormatter
		}]
	};
	
	var dataUrl = "/wmagent/heartbeat";
	
	var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema);
	//writeDebugObject(dataSource)
	//writeEval(dataSource.responseType)
	var tableConfig = WMCore.WebTools.createDefaultTableConfig();
	tableConfig.formatRow = colorRow;
	tableConfig.paginator = {};
	var dataTable = WMCore.WebTools.createDataTable(divID, dataSource, 
	                      WMCore.WebTools.createDefaultTableDef(dataSchema.fields), 
						  tableConfig, 60000);
};