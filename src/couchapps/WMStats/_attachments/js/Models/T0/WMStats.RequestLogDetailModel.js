WMStats.namespace("RequestLogDetailModel");
// don't set the initial view - it is only used for doc retrieval
WMStats.RequestLogDetailModel = new WMStats._ModelBase('', {}, 
                                          WMStats.LogMessage);

WMStats.RequestLogDetailModel.setDBSource(WMStats.T0LogDBCouch);
WMStats.RequestLogDetailModel.setTrigger(WMStats.CustomEvents.ERROR_LOG_LOADED);

$(WMStats.Globals.Event).on(WMStats.CustomEvents.LOG_LOADED, 
    function(event) {
        var options = {"include_docs": true};
    	
    	var logData = WMStats.RequestLogModel.getData();
    	options.keys = logData.getErrorLogIDs();
        
        WMStats.RequestLogDetailModel.retrieveData("allDocs", options);
});