WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestTable = function (requestData, containerDiv) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var tableConfig = {
        "pageLength": 25,
        "scrollX": "",
        "columns": [
            {"title": "D", 
             "defaultContent": 0,
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
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            { "data": "workflow", "title": "workflow"},
            /*
            { "defaultContent": "new",
              "title": "status", 
              "render": function (data, type, row, meta) {
                           
                           var status = row.request_status[row.request_status.length -1].status;
                           var reqSummary = requestData.getSummary(row.workflow);
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
            { "title": "status",
              "render": function (data, type, row, meta) {
              				if (type === "display") {
                            	return formatWorkloadSummarylUrl(row.workflow, 
                                	row.request_status[row.request_status.length -1].status);
                            }
                            return row.request_status[row.request_status.length -1].status;
                          }
            },
            { "title": "duration",
              "render": function (data, type, row, meta) {
              	            if (type === "display") {
	                            var currentTime = Math.round(new Date().getTime() / 1000);
	                            var startTime = row.request_status[row.request_status.length -1].update_time;
	                            return WMStats.Utils.formatDuration(currentTime - startTime);
                            }
                            return row.request_status[row.request_status.length -1].update_time;
                          },
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
              "title": "submitted", 
              "render": function (data, type, row, meta) {
                           var result = requestData.getKeyValue(row.workflow, "status.submitted.first", 0);
                           result += requestData.getKeyValue(row.workflow, "status.submitted.retry", 0);
                           return result;
                          }
            },
			{ "defaultContent": 0,
              "title": "pending", 
              "render": function (data, type, row, meta) {
                           var result = requestData.getKeyValue(row.workflow, "status.submitted.pending", 0);
                           return result;
                          }
            },
            { "defaultContent": 0,
              "title": "running", 
              "render": function (data, type, row, meta) {
                           var result = requestData.getKeyValue(row.workflow, "status.submitted.running", 0);
                           return result;
                          }
            },
            { "defaultContent": 0,
              "title": "cool off ", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            return reqSummary.getTotalCooloff();
                          }
            },
            { "defaultContent": 0,
              "title": "paused", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            return  reqSummary.getTotalPaused();
                          }
            },
            { "defaultContent": 0,
              "title": "failed", 
              "render": function (data, type, row, meta) {
                            var reqSummary = requestData.getSummary(row.workflow);
                            return  reqSummary.getTotalFailure();
                          }
            },
            { "defaultContent": 0,
              "title": "run", 
              "render": function (data, type, row, meta) {
                            return requestData.getKeyValue(row.workflow, "run", 0);
                          }
            }
            //TODO add more data (consult dataops)
        ]
    };
    
    function runNumerDesc(a, b) {
        return (Number(b.run) - Number(a.run));
    }
    
    tableConfig.data = requestData.getList(runNumerDesc);
    
    return WMStats.Table(tableConfig).create(containerDiv, null);
};
