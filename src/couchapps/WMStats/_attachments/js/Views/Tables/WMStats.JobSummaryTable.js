WMStats.namespace("JobSummaryTable");

WMStats.JobSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "dom": '<"top"plf>rt<"bottom"ip>',
        "pageLength": 25,
        "columns": [
            {"title": "L", 
             "defaultContent": 0,
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            {  "render": function (data, type, row, meta) {
                if (type == 'display') {
                    var taskList = row.task.split('/');
                    return taskList[taskList.length - 1];
                }
                return row.task;
            }, "title": "task", "width": "150px"},
            { "data": "status", "title": "status"},
            { "data": "site", "title": "site"},
            { "data": "exitCode", "title": "exit code"},
            { "data": "count", "title": "jobs"},
            { "data": "errorMsg", "title": "error mesage", 
                           "defaultContent": ""},
            { "title": "acdc", defaultContent: "",
              "width": "15px",
              "render": function (data, type, row, meta) {
                            if (type == 'display') {
                                var taskList = row.task.split('/');
                                var endTask = taskList[taskList.length - 1];
                                if (row.status !== "success" && 
                                    !endTask.match(/LogCollect$/) && 
                                    !endTask.match(/Cleanup$/)) {
                                    return WMStats.Utils.formatDetailButton("acdc");
                                }
                            }
                            return "";
                        }
             }
         ],
         "order": [[1, 'asc']]
    };
    
    tableConfig.data = data.getData().status;
    
    var filterConfig = {};

    $(containerDiv).data('workflow', (data.getData()).workflow);
    
    // store the table data
    WMStats.JobSummaryTable.data = WMStats.Table(tableConfig).create(containerDiv, filterConfig);
    return WMStats.JobSummaryTable.data;
};

(function() {
    var vm = WMStats.ViewModel;
    
    vm.JobView.subscribe("data", function() {
        // need to create the lower level view
        var divSelector = vm.JobView.id() + " div.summary_data";
        WMStats.JobSummaryTable(vm.JobView.data(), divSelector);
    });
    
    /*
    vm.AlertJobView.subscribe("data", function() {
        WMStats.JobSummaryTable(vm.AlertJobView.data(), vm.AlertJobView.id());
    });
    */
})();
