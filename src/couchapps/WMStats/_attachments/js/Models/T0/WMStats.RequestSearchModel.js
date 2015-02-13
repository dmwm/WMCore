WMStats.namespace("RequestSearchModel");

WMStats.RequestSearchModel = new WMStats._ModelBase('', 
                                        {"reduce": false, 'include_docs': true}, 
                                        WMStats.WorkloadSummary);

WMStats.RequestSearchModel.setDBSource(WMStats.T0Couch);
WMStats.RequestSearchModel.setTrigger(WMStats.CustomEvents.WORKLOAD_SUMMARY_READY);