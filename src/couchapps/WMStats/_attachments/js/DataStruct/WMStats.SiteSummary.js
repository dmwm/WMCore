WMStats.namespace("SiteSummary");

WMStats.SiteSummary = function() {
    var siteSummary = {numRequests: 0};
    var siteSummary = new WMStats.GenericRequestsSummary(siteSummary);
    
    siteSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.SiteSummary();
        summary.summaryStruct.numRequests = 1;
        summary.jobStatus = doc;
        return summary;
    };
    
    return siteSummary;
}
