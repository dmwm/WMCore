WQ.namespace("WorkloadInfoTable")
/*
 * To do: Maybe needs to add YUI loader for the support library
 */

WQ.WorkloadInfoTable.workloadTable = function(args) {

    var pbs = [];
    var progressFormatter = function (elLiner, oRecord, oColumn, oData) {
        total = oRecord.getData('Jobs')
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
            fields: [{key: "RequestName", label: "Request"},
                     {key: "Team"}, {key: "Jobs", label: "Top level Jobs"},
                     {key: "CompleteJobs", label: "progress",
                      formatter: progressFormatter, parser: "number"}]
            };

    var dataUrl = "./_rewrite/workflowInfo/" + args.workflow

    var dataSource = WQ.createDataSource(dataUrl, dataSchema);
    var tableConfig = WQ.createDefaultTableConfig();
    tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 5});
    /*
    tableConfig.sortedBy ={
        key: "spec_id", dir:YAHOO.widget.DataTable.CLASS_ASC
    };
    */
    var dataTable = WQ.createDataTable(args.divID, dataSource,
                                 WQ.createDefaultTableDef(dataSchema.fields),
                                 tableConfig, 600000);

    // Set up editing flow
    var highlightEditableCell = function(oArgs) {
        var target = oArgs.target;
        var column = this.getColumn(target);
        if (column.key == 'wmspec') {
            this.highlightCell(target);
        };
    };

    var elementTableHandler = function (oArgs) {

        var target = oArgs.target;
        var column = this.getColumn(target);
        if (column.key == 'RequestName') {
            var record = this.getRecord(target);
            var eleTableArgs = {};
            eleTableArgs.divID ="elements";
            eleTableArgs.workflow = record.getData('RequestName');
            args.task(eleTableArgs);
        };
    };

    dataTable.subscribe("cellMouseoverEvent", highlightEditableCell);
    //dataTable.subscribe("cellMouseoutEvent", dataTable.onEventUnhighlightCell);
    dataTable.subscribe("cellClickEvent", elementTableHandler);
};
