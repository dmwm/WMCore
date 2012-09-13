WMStats.namespace("CampaignSummary");

WMStats.CampaignSummary = function() {
    var campaignSummary = {totalEvents: 0,
                           processedEvents: 0,
                           numRequests: 0};
    var campaignSummary = new WMStats.GenericRequestsSummary(campaignSummary);
    
    campaignSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.CampaignSummary();
        summary.summaryStruct.totalEvents = Number(this._get(doc, "input_events", 0));
        summary.summaryStruct.processedEvents = this._get(doc, "output_progress.0.events", 0);
        summary.summaryStruct.numRequests = 1;
        summary.jobStatus = this._get(doc, 'status', {})
        
        return summary;
    };
    
    return campaignSummary;
}