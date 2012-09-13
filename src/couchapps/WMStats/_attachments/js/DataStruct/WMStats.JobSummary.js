WMStats.namespace("JobSummary");

WMStats.JobSummary = function (couchData) {
    
    var jobSummaryData = new WMStats._StructBase();
    
    jobSummaryData.convertCouchData = function(data) {
                                        var jobSummary = {};
                                        jobSummary.status = [];
                                        for (var i in data.rows){
                                            jobSummary.workflow = data.rows[i].key[0];
                                            var statusSummary = {};
                                            statusSummary.status = data.rows[i].key[1];
                                            statusSummary.exitCode = data.rows[i].key[2];
                                            statusSummary.site = data.rows[i].key[3];
                                            if (typeof(statusSummary.site) === "object") {
                                                statusSummary.site = "{}";
                                            }
                                            statusSummary.errorMsg = data.rows[i].key[4];
                                            statusSummary.count = data.rows[i].value;
                                            jobSummary.status.push(statusSummary)
                                        }
                                        return jobSummary;
                                    }
    
    if (couchData) jobSummaryData.setData(couchData);
    
    return jobSummaryData;
}
