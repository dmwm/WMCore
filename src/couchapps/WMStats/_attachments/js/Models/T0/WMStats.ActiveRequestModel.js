WMStats.namespace("ActiveRequestModel");
WMStats.ActiveRequestModel = function() {
    //var initView  = 'tier0Requests';
    //var options = {'include_docs': true};
    var initView = 'bystatus'; 
    var options = {'keys': [
                            "new",
                            "Closed",
                            "Merge",
                            "Harvesting",
                            "Processing Done",
                            "AlcaSkim",
                            "completed"
                            ], 
                   'include_docs': true};
    var reqModel = new WMStats._RequestModelBase(initView, options);
    reqModel.setTrigger(WMStats.CustomEvents.REQUESTS_LOADED);
    // use reqmgrDB source for initial request
    reqModel.setDBSource(WMStats.T0Couch);
    return reqModel;
}();
