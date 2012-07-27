WMStats.namespace("T0");
(function() {
    var initView  = 'tier0Requests';
    var options = {'include_docs': true};
    WMStats.T0.ActiveRequestView = new WMStats._RequestViewBase(initView, options);
    WMStats.T0.ActiveRequestView.setVisualization(WMStats.ActiveRequestTable);
})()
