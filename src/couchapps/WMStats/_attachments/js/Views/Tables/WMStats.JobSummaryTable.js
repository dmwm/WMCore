WMStats.namespace("JobSummaryTable");

WMStats.JobSummaryTable = function (data, containerDiv) {
    
    var tableConfig = {
        "iDisplayLength": 25,
        "aoColumns": [
            /*
            {"sTitle": "D", 
             "sDefaultContent": 0,
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("detail");
                        }},
            */
            {"sTitle": "L", 
             "sDefaultContent": 0,
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            { "mDataProp": function (source, type, val) {
                if (type == 'display') {
                    var taskList = source.task.split('/');
                    return taskList[taskList.length - 1];
                }
                return source.task;
            }, "sTitle": "task", "sWidth": "150px"},
            { "mDataProp": "status", "sTitle": "status"},
            { "mDataProp": "site", "sTitle": "site"},
            { "mDataProp": "exitCode", "sTitle": "exit code"},
            { "mDataProp": "count", "sTitle": "jobs"},
            { "mDataProp": "errorMsg", "sTitle": "error mesage", 
                           "sDefaultContent": ""},
            { "sTitle": "acdc", sDefaultContent: "",
              "sWidth": "15px",
              "mDataProp": function (source, type, val ) {
                            if (type == 'display') {
                                var taskList = source.task.split('/');
                                var endTask = taskList[taskList.length - 1];
                                if (source.status !== "success" && 
                                    !endTask.match(/LogCollect$/) && 
                                    !endTask.match(/Cleanup$/)) {
                                    return WMStats.Utils.formatDetailButton("acdc");
                                }
                            }
                            return "";
                        }
             }
         ],
         "aaSorting": [[1, 'asc']]
    };
    
    tableConfig.aaData = data.getData().status;
    
    var filterConfig = {};

    $(containerDiv).data('workflow', (data.getData()).workflow)
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
