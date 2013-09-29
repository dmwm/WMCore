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
              "sTitle": "event progress", 
              "mDataProp": function ( source, type, val ) {
                           //TODO this might not needed since input_events should be number not string. (for the legacy record)
                           var inputEvents = Number(_activePageData.getKeyValue(taskData.getWorkflow(), "input_events", 1)) || 1;
                           var outputEvents = taskData.getSummary(source[0]).getAvgEvents() || 0;
                           var result = (outputEvents / inputEvents) * 100;
                           return (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "lumi progress", 
              "mDataProp": function ( source, type, val ) {
                           var inputLumis = Number(_activePageData.getKeyValue(taskData.getWorkflow(), "input_lumis", 1)) || 1;
                           var outputLumis = taskData.getSummary(source[0]).getAvgLumis();
                           var result = (outputLumis / inputLumis) * 100;
                           return (result.toFixed(1) + "%");
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
    
    vm.JobView.subscribe("requestName", function() {
        // need to create the lower level view
        var divSelector = vm.JobView.id() + " div.task_summary";
        var tasks = WMStats.ActiveRequestModel.getData().getTasks(vm.JobView.requestName());
        WMStats.TaskSummaryTable(tasks, divSelector);
    });
    
})();
