WMStats.namespace("RunSummary");

WMStats.RunSummary = function() {
    var runSummary = {numRequests: 0};
    var runSummary = new WMStats.GenericRequestsSummary(runSummary);
    
    runSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.RunSummary();
        summary.summaryStruct.numRequests = 1;
        summary.jobStatus = this._get(doc, 'status', {})
        
        return summary;
    };
    
    return runSummary;
}
