WMStats.namespace("JobSummaryView")

WMStats.JobSummaryView = new WMStats._ViewBase('jobsByStatusWorkflow', {}, 
                                          WMStats.JobSummary, WMStats.JobSummaryTable);

WMStats.JobSummaryView.setRequest = function(workflow) {
    this._options = {'reduce': true, 'group_level': 5, 'startkey':[workflow], 
                   'endkey':[workflow, {}]};
}