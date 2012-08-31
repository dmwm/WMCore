WMStats.namespace("ActiveRequestView");
WMStats.ActiveRequestView = function() {
    var initView  = 'tier0Requests';
    var options = {'include_docs': true};
    var reqView = new WMStats._RequestViewBase(initView, options);
    reqView.setVisualization(WMStats.ActiveRequestTable);
    reqView.setTrigger('activeRequestReady');
    return reqView;
}()
