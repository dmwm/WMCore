WMStats.namespace("WorkloadSummaryOutputdataModel")

WMStats.WorkloadSummaryOutputdataModel = new WMStats._ModelBase('summaryByOutputdataset', 
                                  {"reduce": false, 'include_docs': true}, 
                                  WMStats.WorkloadSummary);

WMStats.WorkloadSummaryOutputdataModel.setKey = function(key) {
    this._options= {'include_docs': true, 'reduce': false, 'key': key};
};

WMStats.WorkloadSummaryOutputdataModel.setDBSource(WMStats.WorkloadSummaryCouch)
