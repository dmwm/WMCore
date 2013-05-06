WMStats.namespace("CMSSWSummary");

WMStats.CMSSWSummary = function() {
    var cmsswSummary = {totalEvents: 0,
                           totalLumis: 0,
                           processedEvents: 0,
                           numRequests: 0};
    var cmsswSummary = new WMStats.GenericRequestsSummary(cmsswSummary);
    
    cmsswSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.CMSSWSummary();
        summary.summaryStruct.totalEvents = Number(this._get(doc, "input_events", 0));
        summary.summaryStruct.totalLumis = Number(this._get(doc, "input_lumis", 0));
        summary.summaryStruct.processedEvents = this._get(doc, "output_progress.0.events", 0);
        summary.summaryStruct.progress = this.getAvgProgressSummary(doc)
        summary.summaryStruct.numRequests = 1;
        summary.jobStatus = this._get(doc, 'status', {})
        
        //support legacy code which had cooloff jobs instead cooloff.create, cooloff.submit
        //cooloff.job
        if ((typeof summary.jobStatus.cooloff) === "number") {
            summary.jobStatus.cooloff = {create: 0, submit: 0, job: summary.jobStatus.cooloff}
        }
        return summary;
    };
    
    return cmsswSummary;
};
