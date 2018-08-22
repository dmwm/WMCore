WMStats.namespace("RequestLogModel");
// don't set the initial view - it is only used for doc retrieval
WMStats.RequestLogModel = new WMStats._ModelBase('', {}, 
                                          WMStats.LogDBData);

WMStats.RequestLogModel.setDBSource(WMStats.T0LogDBCouch);
WMStats.RequestLogModel.setTrigger(WMStats.CustomEvents.LOG_LOADED);

$(WMStats.Globals.Event).on(WMStats.CustomEvents.REQUESTS_LOADED, 
    function(event) {
        var options = {"stale": "update_after"};
        
        WMStats.RequestLogModel.retrieveData("logByRequest", options);
});