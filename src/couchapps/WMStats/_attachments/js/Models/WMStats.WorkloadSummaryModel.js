WMStats.namespace("WorkloadSummaryModel");

WMStats.WorkloadSummaryModel = new WMStats._ModelBase('', 
                                        {"reduce": false, 'include_docs': true}, 
                                        WMStats.WorkloadSummary);

WMStats.WorkloadSummaryModel.setDBSource(WMStats.WorkloadSummaryCouch);
WMStats.WorkloadSummaryModel.setTrigger(WMStats.CustomEvents.WORKLOAD_SUMMARY_READY);
