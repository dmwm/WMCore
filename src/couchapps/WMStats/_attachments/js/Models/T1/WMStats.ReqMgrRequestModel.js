WMStats.namespace("ReqMgrRequestModel");
// don't set the initial view - it is only used for doc retrieval
WMStats.ReqMgrRequestModel = new WMStats._ModelBase('', {}, 
                                          WMStats.ReqMgrRequest);

WMStats.ReqMgrRequestModel.setDBSource(WMStats.ReqMgrCouch);
WMStats.ReqMgrRequestModel.setTrigger(WMStats.CustomEvents.RESUBMISSION_SUMMARY_READY);

