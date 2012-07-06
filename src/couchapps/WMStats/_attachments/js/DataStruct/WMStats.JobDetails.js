WMStats.namespace("JobDetails");

WMStats.JobDetails = function (couchData) {
    
    var _data;

    function setData(data) {
        var dataRows = data.rows;
        var jobDetails = [];
        for (var i in dataRows){
            jobDetails.push(dataRows[i].doc);
        }
        _data = jobDetails;
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