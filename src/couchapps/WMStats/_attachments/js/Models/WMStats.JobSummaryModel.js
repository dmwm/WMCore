WMStats.namespace("JobSummaryModel");

WMStats.JobSummaryModel = new WMStats._ModelBase('jobsByStatusWorkflow', {}, 
                                          WMStats.JobSummary);

WMStats.JobSummaryModel.setRequest = function(workflow) {
    this._options = {'reduce': true, 'group_level': 8, 'startkey':[workflow], 
                   'endkey':[workflow, {}]};
};
WMStats.JobSummaryModel.setTrigger([WMStats.CustomEvents.JOB_SUMMARY_READY,
                                    WMStats.CustomEvents.LOADING_DIV_END]);

/*
WMStats.JobSummaryModel.retrieveData = function(workflow) {
    WMStats.JobSummaryModel.setRequest(workflow);
    WMStats.JobSummaryModel.prototype.retrieveData();
}
*/