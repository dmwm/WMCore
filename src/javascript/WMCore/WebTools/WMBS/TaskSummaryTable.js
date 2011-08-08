WMCore.namespace("WMBS.TaskSummaryTable")

WMCore.WMBS.TaskSummaryTable.taskTable = function(oArgs){

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

    var dataUrl = "/wmbsservice/wmbs/tasksummary/" + oArgs.workflow

    var dataSource = WMCore.createDataSource(dataUrl, dataSchema)
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.createDataTable(oArgs.divID, dataSource,
                               WMCore.createDefaultTableDef(dataSchema.fields),
                               WMCore.createDefaultTableConfig(), 1000000)
}