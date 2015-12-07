WMStats.namespace("JobDetailModel");

WMStats.JobDetailModel = new WMStats._ModelBase('jobsByStatusWorkflow', {}, 
                                    WMStats.JobDetails);

WMStats.JobDetailModel.setOptions = function(summary) {
    var startkey = null;
    if ((typeof summary.site) == "object") {
        startkey = [summary.workflow, summary.task, summary.status, summary.exitCode];
    } else if (!summary.acdcURL) {
        startkey = [summary.workflow, summary.task, summary.status, summary.exitCode, summary.site];
    } else {
        startkey = [summary.workflow, summary.task, summary.status, summary.exitCode, summary.site, summary.acdcURL];
    }
    this._options= {'include_docs': true, 'reduce': false, 
              'startkey': startkey,
              'endkey': [summary.workflow, summary.task, summary.status, summary.exitCode, summary.site, {}],
              'limit': 10};
};

WMStats.JobDetailModel.setTrigger([WMStats.CustomEvents.JOB_DETAIL_READY,
                                  WMStats.CustomEvents.LOADING_DIV_END]);
