WMStats.namespace("JobDetailView")

WMStats.JobDetailView = new WMStats._ViewBase('jobsByStatusWorkflow', {}, 
                                    WMStats.JobDetails, WMStats.JobDetailList);

WMStats.JobDetailView.setOptions = function(summary) {
    this._options= {'include_docs': true, 'reduce': false, 
              'startkey': [summary.workflow, summary.status, summary.exitCode, summary.site],
              'endkey': [summary.workflow, summary.status, summary.exitCode, summary.site, {}],
              'limit': 3};
};

WMStats.JobDetailView.setVisualization(WMStats.JobDetailList);