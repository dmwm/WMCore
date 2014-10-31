WMStats.namespace("TaskSummaryTable");

WMStats.TaskSummaryTable = function (data, containerDiv) {
    
    var taskData = data;
    var _activePageData = WMStats.ViewModel.ActiveRequestPage.data();
    var tableConfig = {
        "sDom": '<"top"plf>rt<"bottom"ip>',
        "iDisplayLength": 25,
        "aoColumns": [
            { "mDataProp": function (source, type, val) {
                if (type == 'display') {
                    var taskList = source[0].split('/');
                    return taskList[taskList.length - 1];
                }
                return source;
            }, "sTitle": "task", "sWidth": "150px"},
            { "sDefaultContent": 0,
              "sTitle": "created",
              "mDataProp": function ( source, type, val ) {
                            var taskSummary = taskData.getSummary(source[0]);
                            var jobs = taskSummary.getWMBSTotalJobs();
                            return jobs;
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "queued", 
              "mDataProp": function ( source, type, val ) {
                            var taskSummary = taskData.getSummary(source[0]);
                            var jobs = taskSummary.getTotalQueued();
                            return jobs;
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending ", 
              "mDataProp": function ( source, type, val ) {
                                var taskSummary = taskData.getSummary(source[0]);
                                var jobs = taskSummary.getPending();
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "running ", 
              "mDataProp": function ( source, type, val ) {
                                var taskSummary = taskData.getSummary(source[0]);
                                var jobs = taskSummary.getRunning();
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "success ", 
              "mDataProp": function ( source, type, val ) {
                                var taskSummary = taskData.getSummary(source[0]);
                                var jobs = taskSummary.getJobStatus("success");
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure ", 
              "mDataProp": function ( source, type, val ) {
                                var taskSummary = taskData.getSummary(source[0]);
                                var jobs = taskSummary.getTotalFailure();
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "mDataProp": function ( source, type, val ) {
                                var taskSummary = taskData.getSummary(source[0]);
                                var jobs = taskSummary.getTotalCooloff();
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "paused", 
              "mDataProp": function ( source, type, val ) {
                                var taskSummary = taskData.getSummary(source[0]);
                                var jobs = taskSummary.getTotalPaused();
                                return jobs;
                              }
            },
            { "sDefaultContent": 0,
              "sTitle": "events processed", 
              "mDataProp": function ( source, type, val ) {
                           var outputEvents = taskData.getSummary(source[0]).getAvgEvents() || 0;
                           return outputEvents;
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "lumi processed", 
              "mDataProp": function ( source, type, val ) {
                           var outputLumis = taskData.getSummary(source[0]).getAvgLumis();
                           return outputLumis;
                          }
            }
         ],
         "aaSorting": [[1, 'asc']]
    };
    
    var taskNamesArray = [];
    for (var taskNames in data.getData()){
    	taskNamesArray.push([taskNames]);
    }
    tableConfig.aaData = taskNamesArray;
    
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
