WMStats.namespace("_RequestModelBase");

WMStats._RequestModelBase = function(initView, options) {

    this._initialView = initView || 'bystatus';
    this._options = options || {'include_docs': true};
    this._data = null;
    this._trigger = "requestReady";
    this._dbSource = WMStats.Couch;
};
//Class method
WMStats._RequestModelBase.keysFromIDs = function(data) {
        var keys = [];
        for (var i in data.rows){
            if (data.rows[i].value && data.rows[i].value.id) {
                keys.push(data.rows[i].value.id);
            } else {
                keys.push(data.rows[i].id);
            }
        }
        return keys;
    };

WMStats._RequestModelBase.requestAgentUrlKeys = function(requestList, requestAgentData) {
        var keys = {};
        var requestAgentUrlList = [];
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
   };

WMStats._RequestModelBase.prototype = {
    
    setInitView: function(initView) {
        this._initialView  = initView;
    },
    
    setTrigger: function(triggerName) {
        this._trigger = triggerName;
    },
    
    setDBSource: function(dbSource) {
        this._dbSource = dbSource;
    },
    
    // deprecated
    getData: function() {
        return this._data;
    },
    
    getRequests: function() {
        return this._data;
    },
    
    _getRequestDetailsAndTriggerEvent: function (agentIDs, reqmgrData, objPtr) {
        var options = {'keys': WMStats._RequestModelBase.keysFromIDs(agentIDs), 'reduce': false, 
                       'include_docs': true};
        WMStats.Couch.allDocs(options,
              function(agentData) {
                  //start loading sign disable the filter
                  jQuery(WMStats.Globals.Event).triggerHandler(WMStats.CustomEvents.LOADING_DIV_START);
                  // combine reqmgrData(reqmgr_request) and 
                  // agent_request(agentData) data 
                  var requestCache = WMStats.Requests();
                  requestCache.updateBulkRequests(reqmgrData.rows);
                  requestCache.updateBulkRequests(agentData.rows);
                  
                  // set the data cache
                  objPtr._data = requestCache;
                  // trigger custom events
                  jQuery(WMStats.Globals.Event).triggerHandler(objPtr._trigger, objPtr._data);
              });
    },

    _getLatestRequestAgentUrlAndCreateTable: function (overviewData, keys, objPtr) {
        var options = {"keys": keys, "reduce": false};
        WMStats.Couch.view('latestRequest', options,
              function(agentIDs) {
                  objPtr._getRequestDetailsAndTriggerEvent(agentIDs, overviewData, objPtr);
              });
    },
        
    _getLatestRequestIDsAndCreateTable: function (overviewData, objPtr) {
        /*
         * get list of request ids first from the couchDB then 
         * get the details of the requests.
         */
        var options = {"reduce": true, "group": true, "descending": true};
        var requestList =  WMStats._RequestModelBase.keysFromIDs(overviewData);
        WMStats.Couch.view('requestAgentUrl', options,
              function(requestAgentUrlData) {
                  var keys = WMStats._RequestModelBase.requestAgentUrlKeys(requestList, requestAgentUrlData);
                  objPtr._getLatestRequestAgentUrlAndCreateTable(overviewData, keys, objPtr);
               });
    },

    retrieveData: function (viewName, options) {
        
        if (!viewName) {viewName = this._initialView;}
        if (!options) {options = WMStats.Utils.cloneObj(this._options);}
        var objPtr = this;
        if (viewName == "allDocs") {
            this._dbSource.allDocs(options, function (overviewData) {
                objPtr._getLatestRequestIDsAndCreateTable(overviewData, objPtr);
            });
        } else {
            this._dbSource.view(viewName, options,  function (overviewData) {
                objPtr._getLatestRequestIDsAndCreateTable(overviewData, objPtr);
            });
        }
    },

    clearRequests: function () {
        delete this._data;
    }
};
