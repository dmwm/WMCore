WMStats.namespace("JobSummaryModel")

WMStats.JobSummaryModel = new WMStats._ModelBase('jobsByStatusWorkflow', {}, 
                                          WMStats.JobSummary, WMStats.JobSummaryTable);

WMStats.JobSummaryModel.setRequest = function(workflow) {
    this._options = {'reduce': true, 'group_level': 5, 'startkey':[workflow], 
                   'endkey':[workflow, {}]};
}
WMStats.JobSummaryModel.setTrigger("jobSummaryReady");