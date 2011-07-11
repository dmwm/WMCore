WMCore.namespace("WMBS.WorkflowSummaryTable")

WMCore.WMBS.WorkflowSummaryTable.workflowTable = function(oTArgs){
    var postfixLink = "/wmbsmonitor/template/TaskSummary?workflow=";

    var formatUrl = function(elCell, oRecord, oColumn, sData){
        elCell.innerHTML = "<a href='" + postfixLink + sData + "' target='_blank'>" + sData + "</a>";
        elCell.innerHTML = sData;
    };

    var dataSchema = {
        fields: [{key: "wmspec", formatter: formatUrl},
                 {key: "total_jobs", label: 'total'},
                 {key: "pending"},
                 {key: "processing", label: 'in progress'},
                 {key: "real_fail", label: "fail"},
                 {key: "real_success", label: "success"}]
    };
    var dataUrl = "/wmbsservice/wmbs/workflowsummary";

    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.createDataTable(oTArgs.divID, dataSource,
                               WMCore.createDefaultTableDef(dataSchema.fields),
                               WMCore.createDefaultTableConfig(), 600000);

    // Set up editing flow
    var highlightEditableCell = function(oArgs) {
        var target = oArgs.target;
        var column = this.getColumn(target);
        if (column.key == 'wmspec') {
            this.highlightCell(target);
        };
    };
    
    var taskTableHandler = function (oArgs) {
        
        var target = oArgs.target;
        var column = this.getColumn(target);
        if (column.key == 'wmspec') {
            var record = this.getRecord(target);
            var taskTableArgs = {};
            taskTableArgs.divID ="taskSummary";
            taskTableArgs.workflow = record.getData('wmspec');                
            oTArgs.task(taskTableArgs);
        };
    };

    dataTable.subscribe("cellMouseoverEvent", highlightEditableCell);
    //dataTable.subscribe("cellMouseoutEvent", dataTable.onEventUnhighlightCell);
    dataTable.subscribe("cellClickEvent", taskTableHandler);
};