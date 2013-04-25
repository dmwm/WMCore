WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestTable = function (requestData, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var _activePageData = WMStats.ViewModel.ActiveRequestPage.data();
    
    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "bAutoWidth": false,
        "aoColumns": [
            {"sTitle": "D", 
             "sDefaultContent": 0,
             "sWidth": "15px",
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("detail");
                        }},
            {"sTitle": "L", 
             "sDefaultContent": 0,
             "sWidth": "15px",
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false, "sWidth": "150px"
            },
            { "mDataProp": function (source, type, val) {
                              var requestInfo = _activePageData.getData(source.workflow);
                              return requestInfo.request_status[requestInfo.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            var requestInfo = _activePageData.getData(o.aData.workflow);
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                requestInfo.request_status[requestInfo.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "request_type", "sTitle": "type", "sDefaultContent": ""},
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              var result = _activePageData.getKeyValue(o.aData.workflow, "status.inWMBS",  0) / 
                                          _activePageData.getKeyValue(o.aData.workflow, 'total_jobs', 1) * 100
                              return (result.toFixed(1) + '%');
                        }
            },
            { "sDefaultContent": 0,
              "sTitle": "job progress", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            var totalJobs = reqSummary.getWMBSTotalJobs() || 1;
                            var result = (reqSummary.getJobStatus("success") + reqSummary.getTotalFailure()) /
                                     totalJobs * 100
                            return  (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "event progress", 
              "fnRender": function ( o, val ) {
                           //TODO this might not needed since input_events should be number not string. (for the legacy record)
                           var inputEvents =Number(_activePageData.getKeyValue(o.aData.workflow, "input_events", 1)) || 1;
                           var outputEvents = requestData.getSummary(o.aData.workflow).getAvgEvents();
                           var result = (outputEvents / inputEvents) * 100
                           return (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "lumi progress", 
              "fnRender": function ( o, val ) {
                           var inputLumis =Number(_activePageData.getKeyValue(o.aData.workflow, "input_lumis", 1)) || 1;
                           var outputLumis = requestData.getSummary(o.aData.workflow).getAvgLumis();
                           var result = (outputLumis / inputLumis) * 100
                           return (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure rate", 
              "fnRender": function ( o, val ) {
                           var reqSummary = requestData.getSummary(o.aData.workflow);
                           var totalFailure = reqSummary.getTotalFailure();
                           var totalJobs = (reqSummary.getJobStatus("success") + totalFailure) || 1
                           var result = totalFailure / totalJobs * 100;
                           return (result.toFixed(1)  + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "Eestimated Completion", 
              "fnRender": function ( o, val ) {
                            return (WMStats.Utils.foramtDuration(requestData.estimateCompletionTime(o.aData.workflow)));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return (reqSummary.getTotalCooloff());
                          }
            },
            /*
            { "sDefaultContent": 0,
              "sTitle": "EAT", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            */
            //TODO add more data (consult dataops)
        ]
    }
    
    var filterConfig = {}
    
    tableConfig.aaData = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
