WMStats.namespace("JobSummaryTable");

WMStats.JobSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "sDom": 'lfrtip',
        "aoColumns": [
            { "mDataProp": function (source, type, val) {
                if (type == 'display') {
                    //var taskList = source.task.split('/');
                    //return taskList[taskList.length - 1];
                    return source.task;
                }
                return source.task;
            }, "sTitle": "task", "sWidth": "150px"},
            { "mDataProp": "status", "sTitle": "status"},
            { "mDataProp": "site", "sTitle": "site"},
            { "mDataProp": "exitCode", "sTitle": "exit code"},
            { "mDataProp": "count", "sTitle": "jobs"},
            { "mDataProp": "errorMsg", "sTitle": "error mesage", 
                           "sDefaultContent": ""}
         ],
         "aaSorting": [[1, 'asc']]
    };
    
    tableConfig.aaData = data.getData().status;
    
    var filterConfig = {};

    $(containerDiv).data('workflow', (data.getData()).workflow)
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
