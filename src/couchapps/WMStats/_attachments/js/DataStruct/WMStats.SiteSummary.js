WMStats.namespace("SiteSummary");

WMStats.SiteSummary = function() {
    var siteSummary = {totalEvents: 0,
                       processedEvents: 0,
                       numRequests: 0};
    var siteSummary = new WMStats.GenericRequestsSummary(siteSummary);
    
    siteSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.SiteSummary();
        summary.summaryStruct.totalEvents = Number(this._get(doc, "input_events", 0));
        summary.summaryStruct.processedEvents = this._get(doc, "output_progress.0.events", 0);
        summary.summaryStruct.numRequests = 1;
        summary.jobStatus = doc;
        //support legacy code which had cooloff jobs instead cooloff.create, cooloff.submit
        //cooloff.job
        if ((typeof summary.jobStatus.cooloff) === "number") {
            summary.jobStatus.cooloff = {create: 0, submit: 0, job: summary.jobStatus.cooloff};
        }
        return summary;
    };
    
    return siteSummary;
};
