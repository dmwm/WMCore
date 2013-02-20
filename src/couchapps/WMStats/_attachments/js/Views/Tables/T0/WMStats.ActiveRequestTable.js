WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestTable = function (requestData, containerDiv) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
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
            { "mDataProp": "workflow", "sTitle": "workflow"},
            /*
            { "sDefaultContent": "new",
              "sTitle": "status", 
              "fnRender": function ( o, val ) {
                           
                           var status = o.aData.request_status[o.aData.request_status.length -1].status;
                           var reqSummary = requestData.getSummary(o.aData.workflow);
                           var totalJobs = reqSummary.getWMBSTotalJobs();
                           if (totalJobs > 0) {
                               if (reqSummary.getTotalSubmitted() > 0) {
                                   if (reqSummary.getJobStatus("submitted.running") > 0) {
                                       status = "running";
                                   } else {
                                       status = "submitted";
                                   }
                                   
                               } else {
                                   if (reqSummary.getTotalPaused() > 0) {
                                       status = "paused";
                                   }
                               }
                           }
                           return status
                          }
            },*/
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].update_time
                           }, "sTitle": "duration",
              "fnRender": function ( o, val ) {
                            var currentTime = Math.round(new Date().getTime() / 1000);
                            var startTime = o.aData.request_status[o.aData.request_status.length -1].update_time
                            return WMStats.Utils.foramtDuration(currentTime - startTime)
                          },
              "bUseRendered": false
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
              "sTitle": "submitted", 
              "fnRender": function ( o, val ) {
                           var result = requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.first", 0);
                           result += requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.retry", 0);
                           return result
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                           var result = requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.pending", 0);
                           return result
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                           var result = requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.running", 0);
                           return result
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getTotalCooloff();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "paused", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return  reqSummary.getTotalPaused();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "run", 
              "fnRender": function ( o, val ) {
                            return requestData.getDataByWorkflow(o.aData.workflow, "run", 0);
                          }
            }
            //TODO add more data (consult dataops)
        ]
    }
    
    function runNumerDesc(a, b) {
        return (Number(b.run) - Number(a.run));
    }
    
    tableConfig.aaData = requestData.getList(runNumerDesc);
    
    return WMStats.Table(tableConfig).create(containerDiv, null);
};
