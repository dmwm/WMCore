WMStats.namespace("SiteSummaryTable");

WMStats.SiteSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "pageLength": 50,
        "dom": '<"top"plf>rt<"bottom"ip>',
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
            { "data": "key", "title": "site"},               
            { "render": function (data, type, row, meta) { 
                              return row.summary.summaryStruct.numRequests;
                           }, "title": "requests", "defaultContent": 0, 
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
            }, /*
            { "defaultContent": 0,
              "title": "event progress", 
              "render": function (data, type, row, meta) { 
                           //TODO this might not needed since input_events should be number not string. (for the regacy record)
                           var totalEvents = row.summary.summaryStruct.totalEvents || 1;
                           var result = row.summary.summaryStruct.processedEvents / totalEvents * 100
                            return (result.toFixed(1) + "%");
                          }
            },*/
            { "defaultContent": 0,
              "title": "failure rate", 
              "render": function (data, type, row, meta) {
                            var failJobs =  row.summary.getTotalFailure();
                            var successJobs = row.summary.getJobStatus("success");
                            var totalCompleteJobs = (successJobs + failJobs) || 1;
                            var result = failJobs / totalCompleteJobs * 100;
                            return (result.toFixed(1)  + "%");
                          }
            }
        ]
    };
    tableConfig.data = data.getList();
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv,filterConfig);
};
