WMStats.namespace("Tasks");
WMStats.namespace("TasksSummary");

WMStats.TasksSummary = function(task) {
    
    if (task) {
        var taskSummaryStruct = task.subscription_status || {};
    } else {
        var taskSummaryStruct = {subscription_status: {finished: 0, open:0, total:0}};
    }
    
    
    var taskSummary = new WMStats.GenericRequestsSummary(taskSummaryStruct);
    
    taskSummary.getTaskSummary = function(task) {
        var summaryStruct = {}
        var summary = WMStats.TaskSummary();
        summary.summaryStruct.subscription_status = task.subscription_status;
    }
    
    return taskSummary;
}

WMStats.Tasks = function (tasks) {
    /*
     * Data structure for holding the tasks.
     * tasks structure is 
     * {taskName: {'status': {'success: 0, running:10},
     *             'site': {'T2_US_FNAL': {'success: 0, running:10
     *             'jobtype': 'Processing',
     *             'subscription_status': {finished: 0, open:0, total:0}
     *       }}}
     */
    // request data by workflow name
    this._data = tasks || {};
    this._filter = {};
    this._filteredData = noFilterFlag || WMStats.Tasks(true);
}

WMStats.Tasks.prototype = {
    getData: function () {return this._data},
    getSummary: function(tasks) {
        var taskSummary = TaskSummary();
        for (var taskName in tasks) {
            tasks[taskname]
        }
    }
};
