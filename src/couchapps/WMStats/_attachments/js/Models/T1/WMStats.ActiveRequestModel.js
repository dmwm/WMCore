WMStats.namespace("ActiveRequestModel");
WMStats.ActiveRequestModel = function() {
    var initView = 'bystatus';
    var options = {'keys': [
                            "new",
                            "assignment-approved",
                            "assigned",
                            "ops-hold",
                            "negotiating",
                            "acquired",
                            "running",
                            "running-open",
                            "running-closed",
                            "force-complete",
                            "failed",
                            "epic-FAILED",
                            "completed",
                            "closed-out",
                            "announced",
                            "aborted",
                            "rejected"
                            ], 
                   'include_docs': true};
    var reqModel = new WMStats._RequestModelBase(initView, options);
    reqModel.setTrigger(WMStats.CustomEvents.REQUESTS_LOADED);
    // use reqmgrDB source for initial request
    reqModel.setDBSource(WMStats.ReqMgrCouch);
    
    return reqModel;
}();
