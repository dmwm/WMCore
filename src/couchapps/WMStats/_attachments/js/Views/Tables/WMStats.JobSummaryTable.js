WMStats.namespace("JobSummaryTable");

WMStats.JobSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "sDom": 'lfrtip',
        "aoColumns": [
            { "mDataProp": "task", "sTitle": "task"},
            { "mDataProp": "status", "sTitle": "status"},
            { "mDataProp": "site", "sTitle": "site"},
            { "mDataProp": "exitCode", "sTitle": "exit code"},
            { "mDataProp": "count", "sTitle": "jobs"},
            { "mDataProp": "errorMsg", "sTitle": "error mesage", 
                           "sDefaultContent": ""}
         ]
    };
    
    tableConfig.aaData = data.getData().status;
    
    var filterConfig = {};

    $(containerDiv).data('workflow', (data.getData()).workflow)
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
}