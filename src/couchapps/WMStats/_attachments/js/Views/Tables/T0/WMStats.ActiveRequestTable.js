WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestTable = function (requestData, containerDiv) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "sDom": 'lrtip',
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow"},
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
              "sTitle": "job paused", 
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

    tableConfig.aaData = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, null);
}