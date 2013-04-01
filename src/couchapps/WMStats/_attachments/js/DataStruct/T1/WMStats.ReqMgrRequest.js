WMStats.namespace("ReqMgrRequest");

WMStats.ReqMgrRequest = function (couchData) {
    
    var requestData = new WMStats._StructBase();
    if (couchData) requestData.setData(couchData);
    
    return requestData;
};

