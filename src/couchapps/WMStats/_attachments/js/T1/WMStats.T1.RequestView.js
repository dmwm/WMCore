WMStats.namespace("T1");
(function() {
    var initView  = 'requestByCampaignAndDate';
    var options = {'include_docs': true};
    WMStats.T1.RequestView = new WMStats._RequestViewBase(initView, options);
    WMStats.T1.RequestView.setVisualization(WMStats.DefaultRequestTable);
})()
