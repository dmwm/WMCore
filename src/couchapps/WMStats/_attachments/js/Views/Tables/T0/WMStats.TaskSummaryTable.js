WMStats.namespace("TaskSummaryTable");

WMStats.TaskSummaryTable = function (data, containerDiv) {
    
    var taskData = data;
    var _activePageData = WMStats.ViewModel.ActiveRequestPage.data();
    var tableConfig = {
        "dom": '<"top"plf>rt<"bottom"ip>',
        "pageLength": 25,
        "columns": [
            { "render": function (data, type, row, meta) {
                if (type == 'display') {
                    var taskList = row[0].split('/');
                    return taskList[taskList.length - 1];
                }
                return row;
            }, "title": "task", "sWidth": "150px"},
            { "defaultContent": 0,
              "title": "created",
              "render": function (data, type, row, meta) {
                            var taskSummary = taskData.getSummary(row[0]);
                            var jobs = taskSummary.getWMBSTotalJobs();
                            return jobs;
                          }
            },
            { "defaultContent": 0,
              "title": "queued", 
              "render": function (data, type, row, meta) {
                            var taskSummary = taskData.getSummary(row[0]);
                            var jobs = taskSummary.getTotalQueued();
                            return jobs;
                          }
            },
            { "defaultContent": 0,
              "title": "pending ", 
              "render": function (data, type, row, meta) {
                                var taskSummary = taskData.getSummary(row[0]);
                                var jobs = taskSummary.getPending();
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "running ", 
              "render": function (data, type, row, meta) {
                                var taskSummary = taskData.getSummary(row[0]);
                                var jobs = taskSummary.getRunning();
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "success ", 
              "render": function (data, type, row, meta) {
                                var taskSummary = taskData.getSummary(row[0]);
                                var jobs = taskSummary.getJobStatus("success");
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "failure ", 
              "render": function (data, type, row, meta) {
                                var taskSummary = taskData.getSummary(row[0]);
                                var jobs = taskSummary.getTotalFailure();
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "cool off ", 
              "render": function (data, type, row, meta) {
                                var taskSummary = taskData.getSummary(row[0]);
                                var jobs = taskSummary.getTotalCooloff();
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "paused", 
              "render": function (data, type, row, meta) {
                                var taskSummary = taskData.getSummary(row[0]);
                                var jobs = taskSummary.getTotalPaused();
                                return jobs;
                              }
            },
            { "defaultContent": 0,
              "title": "events processed", 
              "render": function (data, type, row, meta) {
                           var outputEvents = taskData.getSummary(row[0]).getAvgEvents() || 0;
                           return outputEvents;
                          }
            },
            { "defaultContent": 0,
              "title": "lumi processed", 
              "render": function (data, type, row, meta) {
                           var outputLumis = taskData.getSummary(row[0]).getAvgLumis();
                           return outputLumis;
                          }
            }
         ],
         "order": [[1, 'asc']]
    };
    
    var taskNamesArray = [];
    for (var taskNames in data.getData()){
    	taskNamesArray.push([taskNames]);
    }
    tableConfig.data = taskNamesArray;
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};

(function() {
    var vm = WMStats.ViewModel;
    var E = WMStats.CustomEvents;
    
    vm.JobView.subscribe("requestName", function() {
        // empty job detail and resubmission window first
        vm.JobDetail.keys(null, true);
        $(vm.JobDetail.id()).empty();
        $(vm.Resubmission.id()).empty();
 	    // need to create the lower level view
        var divSelector = vm.JobView.id() + " div.task_summary";
        var tasks = WMStats.ActiveRequestModel.getData().getTasks(vm.JobView.requestName());
        WMStats.TaskSummaryTable(tasks, divSelector);
        $(WMStats.Globals.Event).triggerHandler(E.AJAX_LOADING_START);
    });
    
})();
