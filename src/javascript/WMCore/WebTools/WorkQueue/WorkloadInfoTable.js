WMCore.namespace("WorkQueue.WorkloadInfoTable")
/*
 * To do: Maybe needs to add YUI loader for the support library
 */

WMCore.WorkQueue.WorkloadInfoTable.workloadTable = function(divID) {

    var pbs = [];
    var progressFormatter = function (elLiner, oRecord, oColumn, oData) {
        total = oRecord.getData('total')
        if (total === 0 || total === null) {
            total = 1;
            rTotal = 0;
        } else {
            rTotal = total;
        };
        if (oData  === null) {
            oData = 0;
        };
        percent = oData/total*100;
        elLiner.innerHTML = "<div class='percentDiv'>" + percent.toFixed(1) + 
                                "% (" + oData + '/' + rTotal + ")</div>";
        var pb = new YAHOO.widget.ProgressBar({
                            width:'90px',
                            height:'11px',
                            maxValue:total,
                            value: oData 
                        }).render(elLiner);
        pbs.push(pb);
    };
    
    var dataSchema = {
            fields: [{key: "spec_id"}, {key: "spec_name"}, 
                     {key: "owner"}, {key: "total", label: "Top level Jobs"}, 
                     {key: "done", label: "progress", 
                      formatter: progressFormatter, parser: "number"}]
            };
    
    var dataUrl = "/workqueueservice/workqueue/workloadprogress";
    
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);
    var tableConfig = WMCore.createDefaultTableConfig();
    tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 5});
    tableConfig.sortedBy ={
        key: "spec_id", dir:YAHOO.widget.DataTable.CLASS_ASC
    };
    
    var dataTable = WMCore.createDataTable(divID, dataSource, 
                                 WMCore.createDefaultTableDef(dataSchema.fields),
                                 tableConfig, 600000);
};
    