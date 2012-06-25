WMStats.namespace("Utils");

WMStats.Utils.getOrDefault= function (baseObj, objList, val) {
    
    if (baseObj[objList[0]]) { 
        if (objList.length == 1) {
            return baseObj[objList[0]];
        } else {
            return WMStats.Utils.getOrDefault(baseObj[objList[0]], 
                                                         objList.slice(1), val);
        }
    } else {
        return val;
    } 
}

WMStats.Utils.get = function (baseObj, objStr, val) {
    objList = objStr.split('.');
    return WMStats.Utils.getOrDefault(baseObj, objList, val); 
}

WMStats.Utils.formatReqDetailUrl = function (request) {
    return '<a href="' + WMStats.Globals.REQ_DETAIL_URL_PREFIX + 
            encodeURIComponent(request) + '" target="_blank">' + request + '</a>';
}

WMStats.Utils.formatWorkloadSummarylUrl = function (request, status) {
    if (status == "completed" || status == "announced" ||
        status == "closed-out" || status == "archived") {
        return '<a href="' + WMStats.Globals.WORKLOAD_SUMMARY_URL_PREFIX + 
                encodeURIComponent(request) + '" target="_blank">' + status + '</a>';
    } else {
        return status;
    }
}