WMStats.namespace("WorkloadSummary");

WMStats.WorkloadSummary = function (couchData) {
    
    var workloadSummaryData = new WMStats._StructBase();
    
    workloadSummaryData.convertCouchData = function(data) {
                                     var dataRows = data.rows;
                                     var workloadSummary = [];
                                     for (var i in dataRows){
                                         workloadSummary.push(dataRows[i].doc);
                                     }
                                     return workloadSummary;
                                  }
    
    if (couchData) workloadSummaryData.setData(couchData);
    
    return workloadSummaryData
}
