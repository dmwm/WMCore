WMStats.namespace("ActiveRequestModel");
WMStats.ActiveRequestModel = function() {
    var initView = 'requestByStatus'; 
    var options = {'keys': [
                            "new",
                            "assignment-approved",
                            "assigned",
                            "ops-hold",
                            "negotiating",
                            "acquired",
                            "running",
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
    return reqModel;
}();
