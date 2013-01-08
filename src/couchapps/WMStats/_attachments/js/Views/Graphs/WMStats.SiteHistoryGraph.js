WMStats.namespace("SiteHistoryGraph");

WMStats.SiteHistoryGraph = function (historyData, containerDiv) {
    
    var siteHistory = JSON.stringify(historyData);
    var htmlList = '<pre>' + siteHistory + '</pre>';
    $(containerDiv).html(htmlList);
};
