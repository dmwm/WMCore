WMStats.namespace("History");
WMStats.namespace("TimeBucket");

WMStats.TimeBucket = function() {

    var _bucket = new Array();
    var _bucketSize = 24;
    var _interval = 3600; // 1 hour

    function addRow(currentTime, row) {
        var timestamp = row.key[0];
        var couchDoc = row.doc;
        var index = parseInt((currentTime - timestamp) / 3600);
        var updateFlag = false;
        if (_bucket[index] && _bucket[index][couchDoc.agent_url]) {
            if (_bucket[index][couchDoc.agent_url].timestamp < couchDoc.timestamp) {
                updteFlag = true;
            }
        } else if (_bucket[index]) {
            updateFlag = true;
        } else {
            _bucket[index] = {};
            _bucket[index][couchDoc.agent_url] = {};
            _bucket[index][couchDoc.agent_url].requests = {};
            updateFlag = true;
        }
        
        if (updateFlag) {
            _bucket[index][couchDoc.agent_url].timestamp = couchDoc.timestamp;
            _bucket[index][couchDoc.agent_url].requests[couchDoc.workflow] = couchDoc;
        }
    }
    
    function addHistory(couchData) {
        var dataRows = couchData.rows;
        var currentTime = Math.round((new Date()).getTime() / 1000);
        for (var i in dataRows) {
            addRow(currentTime, dataRows[i]);
        }
    }
    
    function getData() {
        return _bucket;
    }
    /*
    function getSiteFailureRate() {
        var requestData = []
        for (var i in _bucket) {
            requestData[i] = {}
            for (var request in _bucket[i]) {
                requestData[i][request]
            }
            requestData[i] = _bucket.requests
        }
    }
    */
    return {
        addHistory: addHistory,
        getData: getData
    };
};

WMStats.History = function (couchData) {
    var _data;
    
    var historyData = new WMStats._StructBase();
    
    historyData.convertCouchData = function(data) {
        var bucket = WMStats.TimeBucket();
        bucket.addHistory(data);
        return bucket.getData();
    };
    
    
    if (couchData) historyData.setData(couchData);
    
    return historyData;
};
