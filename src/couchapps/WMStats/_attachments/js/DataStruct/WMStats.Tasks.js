WMStats.namespace("Tasks");
WMStats.namespace("TaskStruck");
WMStats.namespace("TasksSummary");

WMStats.TasksSummary = function(task) {
    var tier1Summary = {totalEvents: 0,
                        processedEvents: 0};
    if (task) {
        var taskSummaryStruct = task.subscription_status || {};
    } else {
        var taskSummaryStruct = {subscription_status: {finished: 0, open:0, total:0}};
    };
    
    
    var taskSummary = new WMStats.GenericRequestsSummary(taskSummaryStruct);
    
    taskSummary.getAvgProgressSummary = function () {
        
        var progressStat = {};
        var datasets = {};
        var numDataset = 0;
        
        for(var site in task.sites) {
            for (var outputDS in task.sites[site].dataset) {
                if (datasets[outputDS] === undefined) {
                     numDataset += 1;
                     datasets[outputDS] = true;
                }
                WMStats.Utils.updateObj(progressStat, task.sites[site].dataset[outputDS]);
            }
         }

        for (var prop in progressStat) {
            progressStat[prop] = progressStat[prop] / numDataset;
        }
        progressStat.numDataset = numDataset;
        return progressStat;
    };
    
    taskSummary.summaryStruct.progress = taskSummary.getAvgProgressSummary();
    
    taskSummary.jobStatus = task.status;
    
    
    
    return taskSummary;
};

WMStats.TaskStruct = function(taskName, task) {
    this._taskName = taskName;
    this._task = task;
    this._summary = WMStats.TasksSummary(task);
    // number of requests in the data
	this._addJobs = WMStats.Utils.updateObj;
};

WMStats.TaskStruct.prototype = {
   
   getProgressStat: function () {
        var progressStat = {};
        for(var site in this._task.sites) {
        	WMStats.Utils.updateObj(progressStat, this._task.sites[site].dataset);
        }
        return progressStat;
    },
    
    getName: function() {
        return this._taskName;
    },
    
    getSummary: function() {
        return this._summary;
    }
};

WMStats.Tasks = function (tasks, requestName) {
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
    var taskStructDict = {};
    for (var taskName in tasks) {
    	taskStructDict[taskName] = new WMStats.TaskStruct(taskName, tasks[taskName]);
    }
 
    this._data = taskStructDict;
    this._workflow = requestName;
};

WMStats.Tasks.prototype = {
	
	getWorkflow: function() {
		return this._workflow;
	},
    getData: function () {return this._data;},
    			
    getSummary: function(taskName) {
    	// TODO need to add summary case for taskName undefined.
        return this._data[taskName].getSummary();
    }
};
