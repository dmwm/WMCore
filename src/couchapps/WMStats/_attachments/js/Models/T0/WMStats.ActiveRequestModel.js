WMStats.namespace("ActiveRequestModel");
WMStats.ActiveRequestModel = function() {
    var initView  = 'tier0Requests';
    var options = {'include_docs': true};
    var reqModel = new WMStats._RequestModelBase(initView, options);
    reqModel.setVisualization(WMStats.ActiveRequestTable);
    reqModel.setTrigger('activeRequestReady');
    return reqModel;
}()
