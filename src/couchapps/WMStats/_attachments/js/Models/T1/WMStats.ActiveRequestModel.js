WMStats.namespace("ActiveRequestModel");
WMStats.ActiveRequestModel = function() {
    
    var uri = "/wmstatsserver/data/requestcache";
    var reqModel = new WMStats._AjaxModelBase(uri, WMStats.Requests);
    reqModel.setTrigger(WMStats.CustomEvents.REQUESTS_LOADED);
    
    return reqModel;
}();
