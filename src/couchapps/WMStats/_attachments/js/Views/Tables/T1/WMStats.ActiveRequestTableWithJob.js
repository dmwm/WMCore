WMStats.namespace("ActiveRequestTableWithJob");

WMStats.ActiveRequestTableWithJob = function (requestData, containerDiv) {

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
                            return formatReqDetailUrl(o.aData.workflow, o.aData.ReqMgr2Only);
                  }, 
              "bUseRendered": false, "sWidth": "150px"
            },
            { "mDataProp": function (source, type, val) { 
                              var requestInfo = _activePageData.getData(source.workflow);
                              return requestInfo.request_status[requestInfo.request_status.length -1].status;
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            var requestInfo = _activePageData.getData(o.aData.workflow);
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                requestInfo.request_status[requestInfo.request_status.length -1].status);
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
              "mDataProp": function ( source, type, val ) {
                            var reqSummary = requestData.getSummary(source.workflow);
                            var jobs = reqSummary.getTotalQueued();
                            if (type === 'display') {
                                  var requestInfo = _activePageData.getData(source.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, source.workflow, "pending");
                                }
                                return jobs;
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getPending();
                                if (type === 'display') {
                                  var requestInfo = _activePageData.getData(source.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, source.workflow, "running");
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
                                  var requestInfo = _activePageData.getData(source.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, source.workflow, "running");
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
                                  var requestInfo = _activePageData.getData(source.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, source.workflow, "success");
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getTotalFailure();
                                var requestInfo = _activePageData.getData(source.workflow);
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, source.workflow, "failed");
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "mDataProp": function ( source, type, val ) {
                                var reqSummary = requestData.getSummary(source.workflow);
                                var jobs = reqSummary.getTotalCooloff();
                                var requestInfo = _activePageData.getData(source.workflow);
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, source.workflow, "cooloff");
                                }
                                return jobs;
                              }
            },
            { "sDefaultContent": "N/A",
              "sTitle": "GQ", 
              "mDataProp": function ( source, type, val ) {
                                if (type === 'display') {
                                  return WMStats.Globals.getGQLink(source.workflow);
                                }
                                return;
                              }
            },
            { "sDefaultContent": "N/A",
              "sTitle": "LQ", 
              "mDataProp": function ( source, type, val ) {
              					var requestInfo = _activePageData.getData(source.workflow);
                                if (type === 'display') {
                                  if (requestInfo.agent_url) {
                                  	return WMStats.Globals.getLQLink(requestInfo.agent_url, source.workflow);	
                                  }else {
                                  	return "N/A";
                                  }
                                  
                                }
                                return;
                              }
            }
        ]
    };
    
    var filterConfig = {};
    
    tableConfig.aaData = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
