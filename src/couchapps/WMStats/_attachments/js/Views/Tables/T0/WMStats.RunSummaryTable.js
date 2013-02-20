WMStats.namespace("RunSummaryTable");

WMStats.RunSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "aoColumns": [
            {"sTitle": "D", 
             "sDefaultContent": 0,
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("detail");
                        }},
            {"sTitle": "L", 
             "sDefaultContent": 0,
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            { "mDataProp": "key", "sTitle": "run"},               
            { "mDataProp": function (source, type, val) { 
                              return source.summary.summaryStruct.numRequests;
                           }, "sTitle": "requests", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.summaryStruct.runStatus;
                           }, "sTitle": "run status", "sDefaultContent": "Active", 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getJobStatus("success");
                           }, "sTitle": "success", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getTotalFailure();
                           }, "sTitle": "failure", "sDefaultContent": 0, 
            },
            { "sDefaultContent": 0,
              "sTitle": "job progress", 
              "mDataProp": function (source, type, val) { 
                            var totalJobs = source.summary.getWMBSTotalJobs() || 1;
                            var result = (source.summary.getJobStatus("success") + 
                                          source.summary.getTotalFailure()) /
                                          totalJobs * 100
                            if (type === 'display') {
                                return result.toFixed(1) + "%";
                            }
                            return result.toFixed(1);
                          }
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getJobStatus("submitted.pending");
                           }, "sTitle": "pending", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getJobStatus("submitted.running");
                           }, "sTitle": "running", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getTotalCooloff();
                           }, "sTitle": "cool off", "sDefaultContent": 0, 
            },
            { "mDataProp": function (source, type, val) { 
                              return source.summary.getTotalPaused();
                           }, "sTitle": "paused", "sDefaultContent": 0, 
            }
        ]
    }
    
    function runNumerDesc(a, b) {
        return (Number(b.key) - Number(a.key));
    }
    
    tableConfig.aaData = data.getList(runNumerDesc);
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv,filterConfig);
};
