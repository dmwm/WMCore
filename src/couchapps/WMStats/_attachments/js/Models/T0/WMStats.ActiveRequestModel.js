WMStats.namespace("ActiveRequestModel");
WMStats.ActiveRequestModel = function() {
    var initView  = 'tier0Requests';
    var options = {'include_docs': true};
    var reqModel = new WMStats._RequestModelBase(initView, options);
    reqModel.setTrigger(WMStats.CustomEvents.REQUESTS_LOADED);
    return reqModel;
}()
