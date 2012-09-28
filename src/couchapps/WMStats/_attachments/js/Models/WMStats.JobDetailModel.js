WMStats.namespace("JobDetailModel")

WMStats.JobDetailModel = new WMStats._ModelBase('jobsByStatusWorkflow', {}, 
                                    WMStats.JobDetails);

WMStats.JobDetailModel.setOptions = function(summary) {
    this._options= {'include_docs': true, 'reduce': false, 
              'startkey': [summary.workflow, summary.task, summary.status, summary.exitCode, summary.site],
              'endkey': [summary.workflow, summary.task, summary.status, summary.exitCode, summary.site, {}],
              'limit': 3};
};

WMStats.JobDetailModel.setTrigger(WMStats.CustomEvents.JOB_DETAIL_READY);
