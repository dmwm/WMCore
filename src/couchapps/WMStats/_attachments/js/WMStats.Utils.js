WMStats.namespace("Utils");

WMStats.Utils.updateObj = function (baseObj, additionObj, createFlag, updateFunc) {
   /*
    * update baseObj using additonObj.
    * baseObj will be updated but additonObj will the same.
    * updateFuct is the function pointer defines how the object wiil be updated
    * updateFunction takes 3 parameters, baseObj, additonObj, field
    * if udateFunc is not define use addition.
    * createFlag is set to true by default
    */
   
   for (var field in additionObj) {
        if (!baseObj[field]) {
            if (createFlag === undefined || createFlag) {
                baseObj[field] = additionObj[field];
            }
        } else {
            if (typeof(baseObj[field]) == "object"){
                WMStats.Utils.updateObj(baseObj[field], additionObj[field], updateFunc);
            } else {
                if (updateFunc instanceof Function){
                    updateFunc(baseObj, additionObj, field);
                } else {
                    //default is adding
                    baseObj[field] += additionObj[field];
                }
            }
        }
    } 
}
    
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
    if (baseObj === undefined) {
        return val;
    }
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

WMStats.Utils.formatDate = function (timestamp) {
    var date = new Date(timestamp * 1000);
    return (date.getFullYear() + "-" + date.getMonth() + "-" + date.getDate() + 
           " " + date.getHours() + ":" + date.getMinutes());
}

WMStats.Utils.foramtDuration = function (timestamp) {
    if (timestamp == -1) return "N/A";
    var totalMin = Math.floor(timestamp / 60);
    var hours = Math.floor(totalMin / 60);
    var min = totalMin % 60;
    return (hours + " h " + min + " m");
}

WMStats.Utils.createInputFilter = function (selector) {
    // collects the data from input tag  and 
    // create the object which key value
    var a=$(selector).serializeArray(), filter={};
    $.each(a, function(i, obj){
            filter[obj.name] = obj.value;
        });
    return filter;
}