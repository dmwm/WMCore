WMCore.namespace("WorkQueue.WorkloadInfoTable")
/*
 * To do: Maybe needs to add YUI loader for the support library
 */

WMCore.WorkQueue.WorkloadInfoTable.workloadTable = function(divID) {

    var pbs = [];
    var progressFormatter = function (elLiner, oRecord, oColumn, oData) {
                var pb = new YAHOO.widget.ProgressBar({
                    width:'90px',
                    height:'11px',
                    maxValue:100,
                    //className:'some_other_image',
                    value: oData / oRecord.getData("total")
                }).render(elLiner);
                pbs.push(pb);
            };
    
    var dataSchema = {
            fields: [{key: "spec_id"}, {key: "spec_name"}, 
                     {key: "owner"}, {key: "total", label: "Total Elements"}, 
                     {key: "done", label: "progress", formatter: progressFormatter, parser: "number"}]
            };
    
    var dataUrl = "/workqueue/workloadprogress";
    
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);
    var tableConfig = WMCore.createDefaultTableConfig();
    tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 5});
    tableConfig.sortedBy ={
        key: "spec_id", dir:YAHOO.widget.DataTable.CLASS_ASC
    };
    
    var dataTable = WMCore.createDataTable(divID, dataSource, 
                                 WMCore.createDefaultTableDef(dataSchema.fields),
                                 tableConfig, 100000);
};
    