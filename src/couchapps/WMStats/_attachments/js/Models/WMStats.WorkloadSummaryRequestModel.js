WMStats.namespace("WorkloadSummaryRequestModel")

WMStats.WorkloadSummaryRequestModel = new WMStats._ModelBase('', 
                                        {"reduce": false, 'include_docs': true}, 
                                        WMStats.WorkloadSummary);

WMStats.WorkloadSummaryRequestModel.setKey = function(key) {
    this._options= {'include_docs': true, 'reduce': false, 'key': key};
};

WMStats.WorkloadSummaryRequestModel.setDBSource(WMStats.WorkloadSummaryCouch)
