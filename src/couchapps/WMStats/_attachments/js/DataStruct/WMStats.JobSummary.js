WMStats.namespace("JobSummary");

WMStats.JobSummary = function (couchData) {
    
    var _data;

    function setData(data) {
        var jobSummary = {};
        jobSummary.status = [];
        for (var i in data.rows){
            jobSummary.workflow = data.rows[i].key[0];
            var statusSummary = {};
            statusSummary.status = data.rows[i].key[1];
            statusSummary.exitCode = data.rows[i].key[2];
            statusSummary.site = data.rows[i].key[3];
            statusSummary.errorMsg = data.rows[i].key[4];
            statusSummary.count = data.rows[i].value;
            jobSummary.status.push(statusSummary)
        }
        _data = jobSummary;
    }
    
    function getData() {
        return _data;
    }
    
    if (couchData) {
        setData(couchData);
    }
    
    return {
        getData: getData,
        setData: setData
    }
}
