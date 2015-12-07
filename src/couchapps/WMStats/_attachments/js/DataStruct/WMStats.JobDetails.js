WMStats.namespace("JobDetails");

WMStats.JobDetails = function (couchData) {
    
    var jobDetailData = new WMStats._StructBase();
    
    jobDetailData.convertCouchData = function(data) {
                                     var dataRows = data.rows;
                                     var jobDetails = [];
                                     for (var i in dataRows){
                                         jobDetails.push(dataRows[i].doc);
                                     }
                                     return jobDetails;
                                 };
    
    if (couchData) jobDetailData.setData(couchData);
    
    return jobDetailData;
};
