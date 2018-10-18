WMStats.namespace("ActiveRequestTableWithJob");

WMStats.ActiveRequestTableWithJob = function (requestData, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var _activePageData = WMStats.ViewModel.ActiveRequestPage.data();

    var tableConfig = {
        "pageLength": 25,
        "scrollX": "",
        "autoWidth": false,
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
            { "data": "workflow", "title": "workflow",
              "render": function (data, type, row, meta) {
                            return formatReqDetailUrl(row.workflow);
                  }, 
              "width": "150px"
            },
            { "title": "status",
              "render": function (data, type, row, meta) {
                            var requestInfo = _activePageData.getData(row.workflow);
                            if (type === 'display') {
                            	return formatWorkloadSummarylUrl(row.workflow, 
                                	requestInfo.request_status[requestInfo.request_status.length -1].status);
                            }
                            return requestInfo.request_status[requestInfo.request_status.length -1].status;
                          },
            },
            { "render": function (data, type, row, meta) { 
                              var requestInfo = _activePageData.getData(row.workflow);
                              return requestInfo.priority;
                           }, 
              "title": "priority", 
              "defaultContent": 0},
            { "defaultContent": 0,
              "title": "created", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            return reqSummary.getWMBSTotalJobs();
                          }
            },
            { "defaultContent": 0,
              "title": "queued", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            var jobs = reqSummary.getTotalQueued();
                            if (type === 'display') {
                                  var requestInfo = _activePageData.getData(row.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, row.workflow, "pending");
                            }
                            return jobs;
                          }
            },
            { "defaultContent": 0,
              "title": "pending ", 
              "render": function (data, type, row, meta) {
                                var reqSummary = requestData.getSummary(row.workflow);
                                var jobs = reqSummary.getPending();
                                if (type === 'display') {
                                  var requestInfo = _activePageData.getData(row.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, row.workflow, "running");
                                }
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "running ", 
              "render": function (data, type, row, meta) {
                                var reqSummary = requestData.getSummary(row.workflow);
                                var jobs = reqSummary.getRunning();
                                if (type === 'display') {
                                  var requestInfo = _activePageData.getData(row.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, row.workflow, "running");
                                }
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "success ", 
              "render": function (data, type, row, meta) {
                                var reqSummary = requestData.getSummary(row.workflow);
                                var jobs = reqSummary.getJobStatus("success");
                                if (type === 'display') {
                                  var requestInfo = _activePageData.getData(row.workflow);
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, row.workflow, "success");
                                }
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "failure ", 
              "render": function (data, type, row, meta) {
                                var reqSummary = requestData.getSummary(row.workflow);
                                var jobs = reqSummary.getTotalFailure();
                                var requestInfo = _activePageData.getData(row.workflow);
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, row.workflow, "failed");
                                }
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "cool off ", 
              "render": function (data, type, row, meta) {
                                var reqSummary = requestData.getSummary(row.workflow);
                                var jobs = reqSummary.getTotalCooloff();
                                var requestInfo = _activePageData.getData(row.workflow);
                                if (type === 'display') {
                                  return WMStats.Globals.formatJobLink(jobs, requestInfo.agent_url, row.workflow, "cooloff");
                                }
                                return jobs;
                              }
            },
            { "defaultContent": "N/A",
              "title": "GQ", 
              "render": function (data, type, row, meta) {
                                if (type === 'display') {
                                  return WMStats.Globals.getGQLink(row.workflow);
                                }
                                return;
                              }
            },
            { "defaultContent": "N/A",
              "title": "LQ", 
              "render": function (data, type, row, meta) {
              					var requestInfo = _activePageData.getData(row.workflow);
                                if (type === 'display') {
                                  if (requestInfo.agent_url) {
                                  	return WMStats.Globals.getLQLink(requestInfo.agent_url, row.workflow);	
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
    
    tableConfig.data = requestData.getList();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
