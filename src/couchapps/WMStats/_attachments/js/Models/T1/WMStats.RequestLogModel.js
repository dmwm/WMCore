WMStats.namespace("RequestLogModel");
// don't set the initial view - it is only used for doc retrieval
WMStats.RequestLogModel = new WMStats._ModelBase('', {}, 
                                          WMStats.LogDBData);

WMStats.RequestLogModel.setDBSource(WMStats.LogDBCouch);
WMStats.RequestLogModel.setTrigger(WMStats.CustomEvents.ERROR_LOG_LOADED);

$(WMStats.Globals.Event).on(WMStats.CustomEvents.REQUESTS_LOADED, 
    function(event) {
        var options = {};
    	options.group = true;
    	
    	var requestData = WMStats.ActiveRequestModel.getData();
    	options.keys = requestData.getRequestNames();
        
        WMStats.RequestLogModel.retrieveData("latestErrors", options);
});