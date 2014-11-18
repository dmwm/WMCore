WMStats.namespace("SiteSummaryTable");

WMStats.SiteSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "iDisplayLength": 50,
        "sDom": '<"top"plf>rt<"bottom"ip>',
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
            { "mDataProp": "key", "sTitle": "site"},               
            { "mDataProp": function (source, type, val) { 
                              return source.summary.summaryStruct.numRequests;
                           }, "sTitle": "requests", "sDefaultContent": 0, 
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
            }, /*
            { "sDefaultContent": 0,
              "sTitle": "event progress", 
              "mDataProp": function (source, type, val) { 
                           //TODO this might not needed since input_events should be number not string. (for the regacy record)
                           var totalEvents = source.summary.summaryStruct.totalEvents || 1;
                           var result = source.summary.summaryStruct.processedEvents / totalEvents * 100
                            return (result.toFixed(1) + "%");
                          }
            },*/
            { "sDefaultContent": 0,
              "sTitle": "failure rate", 
              "mDataProp": function (source, type, val ) {
                            var failJobs =  source.summary.getTotalFailure();
                            var successJobs = source.summary.getJobStatus("success");
                            var totalCompleteJobs = (successJobs + failJobs) || 1;
                            var result = failJobs / totalCompleteJobs * 100;
                            return (result.toFixed(1)  + "%");
                          }
            }
        ]
    };
    tableConfig.aaData = data.getList();
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv,filterConfig);
};
