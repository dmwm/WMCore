WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestTable = function (requestData, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;

    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "sDom": 'lrtip',
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              var result = requestData.getDataByWorkflow(o.aData.workflow, "status.inWMBS",  0) / 
                                          requestData.getDataByWorkflow(o.aData.workflow, 'total_jobs', 1) * 100
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
                           //TODO this might not needed since input_events should be number not string. (for the regacy record)
                           var inputEvents =Number(requestData.getDataByWorkflow(o.aData.workflow, "input_events", 1)) || 1;
                           var result = Number(requestData.getDataByWorkflow(o.aData.workflow, "output_progress.0.events", 0)) /
                                      (inputEvents) * 100
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
}