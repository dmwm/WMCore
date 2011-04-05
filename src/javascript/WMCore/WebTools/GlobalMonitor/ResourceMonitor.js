WMCore.namespace("RequestManager.ResourceMonitor");

WMCore.RequestManager.ResourceMonitor.resourceInfo = function(divID){
    var dataSchema = {
        fields: [{
            key: "site"
        }, {
            key: "total_slots"
        }, {
            key: "running_jobs",
            label: "jobs in wmagents"
        }]
    };

    var dataUrl = "../monitorSvc/resourcemonitor"

    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);
    var dataTable = WMCore.createDataTable(divID, dataSource,
                                WMCore.createDefaultTableDef(dataSchema.fields),
                                 WMCore.createDefaultTableConfig(), 100000);
}
