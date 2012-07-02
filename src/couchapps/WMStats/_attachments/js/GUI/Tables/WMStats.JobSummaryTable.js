WMStats.namespace("JobSummaryConfig");
WMStats.namespace("JobSummaryTable");

WMStats.JobSummaryConfig = function(data) {
        // jquery datatable config
    var tableConfig = {
        "sDom": 'lfrtip',
        "aoColumns": [
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
    
    return {
        tableConfig: tableConfig,
        filterConfig: filterConfig
    }
}

WMStats.JobSummaryTable = function (data, containerDiv) {
        var config = WMStats.JobSummaryConfig(data);
        $(containerDiv).data('workflow', (data.getData()).workflow)
        return WMStats.Table(config.tableConfig).create(containerDiv, 
                                                 config.filterConfig);
}