WMStats.namespace("LogDBData");

WMStats.LogDBData = function (couchData) {
    
    var logDBData = new WMStats._StructBase();
    logDBData._sortedLogs = {};
    logDBData._errorLogIDList =[];
    logDBData._errorLogs = [];

    logDBData._timestampSort = function(a, b) {
    	// sort the record by descending order
        return (a.ts - b.ts);
    };
    
    logDBData._getValue = function(obj, key, defaultType) {
    	if (obj.key === undefined) {
    		if (defaultType !== undefined) {
    			obj[key] = defaultType;
    		} else {
    			obj[key] = {};	
    		}
    	} 
    	return obj[key];
    };
    
    logDBData._getThreadValue = function(obj, request, agent, thread) {
    	requestObj = this._getValue(this._sortedLogs, request);
    	agentObj = this._getValue(requestObj, agent);
    	threadObj = this._getValue(agentObj, thread, []);
    	return threadObj;
    };
    
    logDBData.convertCouchData = function (couchData) {
        var results = [];
        if (couchData.rows) {
        	for (var i in couchData.rows) {
        		row = couchData.rows[i]['value'];
        		row.request = couchData.rows[i]['key'];
        		row.id = couchData.rows[i].id;	
        		results.push(row);
        	}	
        }
        // and sortby ts stamp decending order
        results.sort(this._timestampSort); 
        return results;
    },
    
    logDBData._setLogWithLastestError = function () {
    	
    	for (var i in this._data) {
    		 thrResult = this._getThreadValue(this._sortedLogs, this._data[i].request,
    		 	                             this._data[i].agent, this._data[i].thr);
    		 if (this._data[i].type !== "message") {
    		 	thrResult.push(this._data[i]);	
    		 };
    	};
    	
    	var logs = this._sortedLogs;
    	for (var req in this._sortedLogs) {
    		for (var agent in logs[req]) {
    			for (var thread in logs[req][agent]) {
    			    for (var i in logs[req][agent][thread]) {
    					if (logs[req][agent][thread][i].type === "agent-error" ||
    					    logs[req][agent][thread][i].type === "agent-warning") {
    						this._errorLogs.push(logs[req][agent][thread][i]);
    						this._errorLogIDList.push(logs[req][agent][thread][i].id);
    					}
    				}
    			}
    		}
    	}
    	
    	return this._errorLogs;
    };
    
    logDBData.getErrorLogIDs = function () {
    	return this._errorLogIDList;
    };
    
    logDBData.setMessages = function (request, agent, thread, messages) {
    	this._sortedLogs[request][agent][thread][0].messages = messages;
    };
    
    logDBData.getLogWithLastestError = function() {
    	return this._errorLogs;
    };
    
    if (couchData) logDBData.setData(couchData);
    
    logDBData._setLogWithLastestError();
    
    return logDBData;
};
