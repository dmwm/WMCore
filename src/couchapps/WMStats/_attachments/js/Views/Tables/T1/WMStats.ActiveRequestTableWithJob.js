WMStats.namespace("ActiveRequestTableWithJob");

WMStats.ActiveRequestTableWithJob = function (requestData, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;

    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "sDom": 'lrtip',
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
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "sDefaultContent": 0,
              "sTitle": "created", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getWMBSTotalJobs();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending ", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getPending();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running ", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getRunning();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "success", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getJobStatus("success");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure", 
              "fnRender": function ( o, val ) {
                           var reqSummary = requestData.getSummary(o.aData.workflow);
                           return reqSummary.getTotalFailure();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getTotalCooloff();
                          }
            }
        ]
    }
    
    var filterConfig = {}
    
    tableConfig.aaData = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
