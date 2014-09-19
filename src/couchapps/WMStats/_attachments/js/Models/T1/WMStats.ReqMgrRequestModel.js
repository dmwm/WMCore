WMStats.namespace("ReqMgrRequestModel");
WMStats.ReqMgrRequestModel = new WMStats._ModelBase('jobsByStatusWorkflow', {}, 
                                          WMStats.ReqMgrRequest);

WMStats.ReqMgrRequestModel.setDBSource(WMStats.ReqMgrCouch);
WMStats.ReqMgrRequestModel.setTrigger(WMStats.CustomEvents.RESUBMISSION_SUMMARY_READY);

