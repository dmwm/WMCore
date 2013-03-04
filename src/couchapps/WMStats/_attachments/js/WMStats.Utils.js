WMStats.namespace("Utils");

    /* simple function to clone obj only works with no prototype */
WMStats.Utils.cloneObj = function(sourceObj) {
        if (typeof sourceObj === "object"){
            var newObj = sourceObj.constructor();
            for (var prop in sourceObj) {
                newObj[prop] = WMStats.Utils.cloneObj(sourceObj[prop]);
            }
            return newObj;
        } else {
            return sourceObj;
        }
    };
    
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
                baseObj[field] = WMStats.Utils.cloneObj(additionObj[field]);
            }
        } else {
            if (typeof(baseObj[field]) == "object"){
                WMStats.Utils.updateObj(baseObj[field], additionObj[field], createFlag, updateFunc);
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
            encodeURIComponent(request) + '" target="requestDetailFrame">' + request + '</a>';
}

WMStats.Utils.formatWorkloadSummarylUrl = function (request, status) {
    if (status === undefined) {
        return '<a href="' + WMStats.Globals.WORKLOAD_SUMMARY_URL_PREFIX + 
                encodeURIComponent(request) + '" target="workloadSummaryFrame">' + request + '</a>';
    } else if (status == "completed" || status == "announced" ||
        status == "closed-out" || status.indexOf("archived") !== -1) {
        return '<a href="' + WMStats.Globals.WORKLOAD_SUMMARY_URL_PREFIX + 
                encodeURIComponent(request) + '" target="workloadSummaryFrame">' + status + '</a>';
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

WMStats.Utils.formatDetailButton = function (name) {
    return '<div class="detailButton" name="'+ name + '"></div>';
}


WMStats.Utils.utcClock = function(date) {
    
    function appendZero(num) {
        if (num < 10) {
            return "0" + num;
        }
        return num
    }
    var day = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    var now;
    if (date === undefined) {
        now = new Date(); 
    }else {
        now = date;
    }
    var month =  now.getUTCMonth() + 1;
    var utcString = now.getUTCFullYear() + "/" + month + "/" + 
                    now.getUTCDate() + " (" + day[now.getUTCDay()] + ") " +
                    appendZero(now.getUTCHours())  + ":" + appendZero(now.getUTCMinutes()) + ":" +
                    appendZero(now.getUTCSeconds()) + " UTC";
    return utcString;
};

WMStats.Utils.expandFormat = function(dataArray, summaryStr, formatFunc) {
    var htmlstr = "";
    if (dataArray == undefined || typeof (dataArray) !== "object" || dataArray.length == 0) {
        htmlstr +=  "<strong>" +  summaryStr + ":</strong>";
     } else {
        htmlstr += "<details> <summary><strong>" + summaryStr +"</strong></summary><ul>";
        if (formatFunc === undefined) {
            formatFunc = function(x) {return x};
        }
        for (var i in dataArray) {
            htmlstr += "<li>" + formatFunc(dataArray[i], i) + "</li>";
        }
        htmlstr += "</ul></details>";
    }
    return htmlstr;
}


WMStats.Utils.largeNumberFormat = function(number) {
    var mega = 1000000;
    var giga = 1000000000;
    if (number < mega) {
        return number;
    } else if (number < giga) {
        return (number/mega).toFixed(1) + " M";
    } else {
        return (number/giga).toFixed(1) + " G";
    }
}
