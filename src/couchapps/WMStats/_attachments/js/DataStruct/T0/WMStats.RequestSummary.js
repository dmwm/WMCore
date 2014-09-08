WMStats.namespace("Requests");
WMStats.namespace("RequestsSummary");

WMStats.RequestsSummary = function() {
    //TODO add specific tier0 summary structure
    var tier0Summary = {};
    var requestSummary = new WMStats.GenericRequestsSummary(tier0Summary);
    return requestSummary;
};

WMStats.Requests = function(noFilterFlag) {
    var tier0Requests = new WMStats.GenericRequests(noFilterFlag);
    return tier0Requests;
};
