WMStats.namespace("RunSummaryTable");

WMStats.RunSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "pageLength": 25,
        "scrollX": "",
        "columns": [
            {"title": "D", 
             "defaultContent": 0,
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("detail");
                        }},
            {"title": "L", 
             "defaultContent": 0,
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            { "data": "key", "title": "run"},               
            { "render": function (data, type, row, meta) { 
                              return row.summary.summaryStruct.numRequests;
                           }, "title": "requests", "defaultContent": 0, 
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.summaryStruct.runStatus;
                           }, "title": "run status", "defaultContent": "Active", 
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.getJobStatus("success");
                           }, "title": "success", "defaultContent": 0, 
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.getTotalFailure();
                           }, "title": "failure", "defaultContent": 0, 
            },
            { "defaultContent": 0,
              "title": "job progress", 
              "render": function (data, type, row, meta) { 
                            var totalJobs = row.summary.getWMBSTotalJobs() || 1;
                            var result = (row.summary.getJobStatus("success") + 
                                          row.summary.getTotalFailure()) /
                                          totalJobs * 100;
                            if (type === 'display') {
                                return result.toFixed(1) + "%";
                            }
                            return result.toFixed(1);
                          }
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.getJobStatus("submitted.pending");
                           }, "title": "pending", "defaultContent": 0, 
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.getJobStatus("submitted.running");
                           }, "title": "running", "defaultContent": 0, 
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.getTotalCooloff();
                           }, "title": "cool off", "defaultContent": 0, 
            },
            { "render": function (data, type, row, meta) { 
                              return row.summary.getTotalPaused();
                           }, "title": "paused", "defaultContent": 0, 
            }
        ]
    };
    
    function runNumerDesc(a, b) {
        return (Number(b.key) - Number(a.key));
    }
    
    tableConfig.data = data.getList(runNumerDesc);
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv,filterConfig);
};
