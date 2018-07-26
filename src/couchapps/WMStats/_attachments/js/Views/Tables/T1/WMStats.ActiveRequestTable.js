WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestTable = function (requestData, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var _activePageData = WMStats.ViewModel.ActiveRequestPage.data();
    
    var tableConfig = {
        "pageLength": 25,
        "scrollX": true,
        "columns": [
            {"title": "D", 
             "defaultContent": 0,
             "width": "15px",
             "render": function (data, type, row, meta) {
             			  if (type === "display") {
                             return WMStats.Utils.formatDetailButton("detail", row.skipped);
                          }
                          if (row.skipped) {
                          	 return 1;
                          }
                        }},
            {"title": "L", 
             "defaultContent": 0,
             "width": "15px",
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            { "data": "workflow", 
              "title": "workflow",
              "render": function (data, type, row, meta) {
                            return formatReqDetailUrl(data);
                      }, 
              "width": "150px"
            },
            { "title": "status",
              "render": function (data, type, row, meta) {
                            var requestInfo = _activePageData.getData(row.workflow);
                            return formatWorkloadSummarylUrl(row.workflow, 
                                requestInfo.getLastState());
                          },
            },
            { "title": "type",
              "render": function (data, type, row, meta) { 
                            var requestInfo = _activePageData.getData(row.workflow);
                            return requestInfo.request_type;
                           }, 
             },
            { "render": function (data, type, row, meta) { 
                              var requestInfo = _activePageData.getData(row.workflow);
                              return requestInfo.priority;
                           }, 
              "title": "priority", 
              "defaultContent": 0,
              "type": "num"},
            { "defaultContent": 0,
              "title": "queue injection",  
              "render": function (data, type, row, meta) {
                              var result = _activePageData.getKeyValue(row.workflow, "status.inWMBS",  0) / 
                                          _activePageData.getKeyValue(row.workflow, 'total_jobs', 1) * 100;
                              return (result.toFixed(1) + '%');
                        }
            },
            { "defaultContent": 0,
              "title": "job progress", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            var totalJobs = reqSummary.getWMBSTotalJobs() || 1;
                            var result = (reqSummary.getJobStatus("success") + reqSummary.getTotalFailure()) /
                                     totalJobs * 100;
                            return  (result.toFixed(1) + "%");
                         },
               "type": "num-fmt"
            },
            { "defaultContent": 0,
              "title": "event progress", 
              "render": function (data, type, row, meta) {
                           //TODO this might not needed since input_events should be number not string. (for the legacy record)
                           var inputEvents =Number(_activePageData.getKeyValue(row.workflow, "input_events", 1)) || 1;
                           var outputEvents = requestData.getSummary(row.workflow).getAvgEvents();
                           var result = (outputEvents / inputEvents) * 100;
                           return (result.toFixed(1) + "%");
                          },
               "type": "num-fmt"
            },
            { "defaultContent": 0,
              "title": "lumi progress", 
              "render": function (data, type, row, meta) {
                           var inputLumis =Number(_activePageData.getKeyValue(row.workflow, "input_lumis", 1)) || 1;
                           var outputLumis = requestData.getSummary(row.workflow).getAvgLumis();
                           var result = (outputLumis / inputLumis) * 100;
                           return (result.toFixed(1) + "%");
                          },
               "type": "num-fmt"
            },
            { "defaultContent": 0,
              "title": "failure rate", 
              "render": function (data, type, row, meta) {
                           var reqSummary = requestData.getSummary(row.workflow);
                           var totalFailure = reqSummary.getTotalFailure();
                           var totalJobs = (reqSummary.getJobStatus("success") + totalFailure) || 1;
                           var result = totalFailure / totalJobs * 100;
                           return (result.toFixed(1)  + "%");
                          },
               "type": "num-fmt"
            },
            { "defaultContent": 0,
              "title": "Eestimated Completion", 
              "render": function (data, type, row, meta) {
                            return (WMStats.Utils.formatDuration(requestData.estimateCompletionTime(row.workflow)));
                          },
               "type": "num-fmt"
            },
            { "defaultContent": 0,
              "title": "cool off ", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            return (reqSummary.getTotalCooloff());
                          },
               "type": "num"
            }
            //TODO add more data (consult dataops)
        ]
    };
    
    var filterConfig = {};
    
    tableConfig.data = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
