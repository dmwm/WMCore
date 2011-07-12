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

    //workqueue database name is hardcoded, need to change to get from config
    //This makes this javascript not reusable but solves the path issue on
    //different deployment (using proxy, rewrite rules.
    var dataUrl = "workflowInfo?request=" + args.workflow
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
    // TODO: not sure why this is not working
    var highlightRequestCell = function(oArgs) {
        var column = this.getColumn(oArgs.target);
        if (column.key == 'RequestName') {
            this.onEventHighlightCell(oArgs.event, oArgs.target);
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

    dataTable.subscribe("cellMouseoverEvent", dataTable.onEventHighlightCell);
    dataTable.subscribe("cellMouseoutEvent", dataTable.onEventUnhighlightCell);
    dataTable.subscribe("cellClickEvent", elementTableHandler);
};
