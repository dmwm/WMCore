WMStats.namespace("UserSummary");

WMStats.UserSummary = function() {
    var userSummary = {numRequests: 0};
    var userSummary = new WMStats.GenericRequestsSummary(userSummary);
    
    userSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.UserSummary();
        summary.summaryStruct.numRequests = 1;
        summary.jobStatus = this._get(doc, 'status', {})
        
        return summary;
    };
    
    return userSummary;
};
