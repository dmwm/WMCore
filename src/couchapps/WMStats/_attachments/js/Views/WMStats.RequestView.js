WMStats.namespace("_RequestViewBase");
WMStats.namespace("RequestView");

WMStats._RequestViewBase = function(initView, options, visFunc) {

    this._initialView = initView || 'requestByCampaignAndDate';
    this._options = options || {'include_docs': true};
    this._visFunc = visFunc || null;
    this._containerDiv = null;
    this._data = null;
}
//Class method
WMStats._RequestViewBase.keysFromIDs = function(data) {
        var keys = [];
        for (var i in data.rows){
            keys.push(data.rows[i].value.id);
        }
        return keys;      
    }

WMStats._RequestViewBase.requestAgentUrlKeys = function(requestList, requestAgentData) {
        var keys = {};
        var requestAgentUrlList = []
        for (var i in requestAgentData.rows){
            var request = requestAgentData.rows[i].key[0];
            if (!keys[request]) {
                keys[request] = [];
            }
            keys[request].push(requestAgentData.rows[i].key[1]);
        }
        
        for (var j=0; j < requestList.length; j++) {
            for (var k in keys[requestList[j]]) {
                requestAgentUrlList.push([requestList[j], keys[requestList[j]][k]]);
            }
        }
        return requestAgentUrlList;
    }

WMStats._RequestViewBase.prototype = {

    setVisualization: function(visFunc) {
        //visFunc take 2 args (requestData, containerDiv)
        // requestData is instance of WMStatsRequests
        this._visFunc = visFunc;
    },
    
    getData: function() {
        return this._data;
    },
    
    _getRequestDetailsAndCreateTable: function (agentIDs, reqmgrData, objPtr) {
        var options = {'keys': WMStats._RequestViewBase.keysFromIDs(agentIDs), 'reduce': false, 
                       'include_docs': true};
        WMStats.Couch.allDocs(options,
              function(agentData) {
                  // combine reqmgrData(reqmgr_request) and 
                  // agent_request(agentData) data 
                  var requestCache = WMStats.Requests()
                  requestCache.updateBulkRequests(reqmgrData.rows)
                  requestCache.updateBulkRequests(agentData.rows)
                  
                  // set the data cache
                  objPtr._data = requestCache;
                  // trigger custom events
                  jQuery(objPtr._containerDiv).trigger('requestDataReady', objPtr._data)
                  // create gui
                  return objPtr._visFunc(objPtr._data, objPtr._containerDiv);
              })
    },

    _getLatestRequestAgentUrlAndCreateTable: function (overviewData, keys, objPtr) {
        var options = {"keys": keys, 
                       "reduce": true, "group": true, "descending": true};
        WMStats.Couch.view('latestRequest', options,
              function(agentIDs) {
                  objPtr._getRequestDetailsAndCreateTable(agentIDs, overviewData, objPtr)
              })
    },
        
    _getLatestRequestIDsAndCreateTable: function (overviewData, objPtr) {
        /*
         * get list of request ids first from the couchDB then 
         * get the details of the requests.
         */
        var options = {"reduce": true, "group": true, "descending": true};
        var requestList =  WMStats._RequestViewBase.keysFromIDs(overviewData);
        WMStats.Couch.view('latestRequest', options,
              function(requestAgentUrlData) {
                  var keys = WMStats._RequestViewBase.requestAgentUrlKeys(requestList, requestAgentUrlData)
                  objPtr._getLatestRequestAgentUrlAndCreateTable(overviewData, keys, objPtr)
                })
    },

    draw: function (selector, viewName, options) {
        if (!viewName) {viewName = this._initialView;}
        if (!options) {options = this._options;}
        this._containerDiv = selector;
        var objPtr = this;
        if (viewName == "allDocs") {
            WMStats.Couch.allDocs(options, function (overviewData) {
                objPtr._getLatestRequestIDsAndCreateTable(overviewData, objPtr)
            });
        } else {
            WMStats.Couch.view(viewName, options,  function (overviewData) {
                objPtr._getLatestRequestIDsAndCreateTable(overviewData, objPtr)
            });
        }
    },
    
    clearData: function () {
        delete this._data;
    }
};

(function($){
    if (WMStats.Globals.VARIANT == "tier1") {
        WMStats.Globals.loadScript("js/T1/WMStats.T1.RequestView.js", 
                            function() {
                                WMStats.RequestView = WMStats.T1.RequestView;
                                })
    } else if (WMStats.Globals.VARIANT == "tier0") {
        WMStats.Globals.loadScript("js/T0/WMStats.T0.RequestView.js", 
                            function() {
                                WMStats.RequestView =  WMStats.T0.RequestView;
                                })
    } else if (WMStats.Globals.VARIANT == "analysis") {
        WMStats.Globals.loadScript("js/Analysis/WMStats.Analysis.RequestView.js", 
                            function() {
                                WMStats.RequestView =  WMStats.Analysis.RequestView;
                                })
    } 
})(jQuery)
