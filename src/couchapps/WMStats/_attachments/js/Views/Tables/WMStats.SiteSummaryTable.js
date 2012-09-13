WMStats.namespace("SiteSummaryTable");

WMStats.SiteSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "sScrollX": "",
        "aoColumns": [
            { "mDataProp": "key", "sTitle": "site"},               
            { "mDataProp": function (source, type, val) { 
                              return source.summary.summaryStruct.numRequests;
                           }, "sTitle": "requests", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getJobStatus("success");
                           }, "sTitle": "success", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.summaryStruct.totalEvents;
                           }, "sTitle": "events", "sDefaultContent": 0, 
            }
        ]
    }
    tableConfig.aaData = data.getList();
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv,filterConfig);
}