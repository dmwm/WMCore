WMStats.namespace("ActiveRequestTableWithJob");

WMStats.ActiveRequestTableWithJob = function (requestData, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;

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
              "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getTotalQueued();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getPending();
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, source.agent_url, source.workflow, "pending")
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "running ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getRunning();
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, source.agent_url, source.workflow, "running")
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "success ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getJobStatus("success");
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, source.agent_url, source.workflow, "success")
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getTotalFailure();
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, source.agent_url, source.workflow, "failed")
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getTotalCooloff();
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, source.agent_url, source.workflow, "cooloff")
                                }
                                return jobs;
                              }
            }
        ]
    }
    
    var filterConfig = {}
    
    tableConfig.aaData = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
