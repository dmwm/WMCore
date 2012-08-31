WMStats.namespace("RequestView");
WMStats.RequestView = function() {
    var initView  = 'requestByCampaignAndDate';
    var options = {'include_docs': true};
    var reqView = new WMStats._RequestViewBase(initView, options);
    reqView.setVisualization(WMStats.DefaultRequestTable);
    reqView.setTrigger('requestReady');
    return reqView;
}()
