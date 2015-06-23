WMStats.namespace("LogDBData");

WMStats.LogDBData = function (couchData) {
    
    var logDBData = new WMStats._StructBase();
    
    logDBData.convertCouchData = function(couchData) {
        // defaut conversion need to be over written
        if (couchData.rows) {
        	return couchData.rows;
        } else {
        	return [];
        }
    };
    
    if (couchData) logDBData.setData(couchData);
    
    return logDBData;
};
