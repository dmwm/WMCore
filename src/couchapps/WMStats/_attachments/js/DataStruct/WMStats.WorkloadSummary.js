WMStats.namespace("WorkloadSummary");

WMStats.WorkloadSummary = function (couchData) {
    
    var workloadSummaryData = new WMStats._StructBase();
    
    workloadSummaryData.convertCouchData = function(data) {
                                     var dataRows = data.rows;
                                     var workloadSummary = [];
                                     for (var i in dataRows){
                                     	 var wmstatsDoc = WMStats.Globals.convertRequestDocToWMStatsFormat(dataRows[i].doc);
                                         workloadSummary.push(wmstatsDoc);
                                     }
                                     return workloadSummary;
                                 };
    
    if (couchData) workloadSummaryData.setData(couchData);
    
    return workloadSummaryData;
};
