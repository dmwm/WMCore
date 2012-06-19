WMStats.namespace("_RequestViewBase");
WMStats.namespace("RequestView");

WMStats._RequestViewBase = function(initView, options, tableConfig, filterConfig) {

    this._initialView = initView || 'requestByCampaignAndDate';
    this._options = options || {'include_docs': true};
    var _get = WMStats._RequestViewBase.get;
    var formatReqDetailUrl = WMStats._RequestViewBase.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats._RequestViewBase.formatWorkloadSummarylUrl
    var defaultTableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset",
                           "sDefaultContent": ""},
            { "mDataProp": "site_white_list", "sTitle": "site white list",
                           "sDefaultContent": "",
              "fnRender": function ( o, val ) {
                            if (o.aData.site_white_list.length > 1) {
                                return "mulitple sites";
                            } else {
                                return o.aData.site_white_list[0];
                            }
                          },
              "bUseRendered": false,
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.queued.first", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.pending", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.failure.create", 0) + 
                                    _get(o.aData, "status.failure.submit", 0) + 
                                    _get(o.aData, "status.failure.exception", 0));
                          }
            },
            
            { "sDefaultContent": 0,
              "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.canceled", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.success", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "cooloff", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.cooloff", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "prev cooloff",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.submitted.retry", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return (_get(o.aData, "status.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1) * 100 + '%');
                        }
            }
            
            //TODO add more data (consult dataops)
        ]
    }
    
    var defaultFilterConfig = {
        "aoColumns": [
            {type: "text", bRegex: true, bSmart: true},               
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true}
        ]
    }
    this.tableConfig = tableConfig || defaultTableConfig;
    this.filterConfig = filterConfig || defaultFilterConfig;
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
        
        for (var j; j < requestList.length; j++) {
            for (var k in keys[requestList[j]]) {
                requestAgentUrlList.push([requestList[j], keys[requestList[j]][k]]);
            }
        }
        return requestAgentUrlList;
    }

WMStats._RequestViewBase.getOrDefault= function (baseObj, objList, val) {
    
    if (baseObj[objList[0]]) { 
        if (objList.length == 1) {
            return baseObj[objList[0]];
        } else {
            return WMStats._RequestViewBase.getOrDefault(baseObj[objList[0]], 
                                                         objList.slice(1), val);
        }
    } else {
        return val;
    } 
}

WMStats._RequestViewBase.get = function (baseObj, objStr, val) {
    objList = objStr.split('.');
    return WMStats._RequestViewBase.getOrDefault(baseObj, objList, val); 
}

WMStats._RequestViewBase.formatReqDetailUrl = function (request) {
    return '<a href="' + WMStats.Globals.REQ_DETAIL_URL_PREFIX + 
            encodeURIComponent(request) + '" target="_blank">' + request + '</a>';
}

WMStats._RequestViewBase.formatWorkloadSummarylUrl = function (request, status) {
    if (status == "completed" || status == "announced" ||
        status == "closed-out" || status == "archived") {
        return '<a href="' + WMStats.Globals.WORKLOAD_SUMMARY_URL_PREFIX + 
                encodeURIComponent(request) + '" target="_blank">' + status + '</a>';
    } else {
        return status;
    }
}


WMStats._RequestViewBase.prototype = {

    getData: function() {
        return this._data;
    }
    ,
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
                  objPtr._data = requestCache.getList();
                  
                  //create table
                  objPtr.tableConfig.aaData = objPtr._data;
                  var selector = objPtr._containerDiv + " table";
                  return WMStats.Table(objPtr.tableConfig).create(selector, 
                                                         objPtr.filterConfig);
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

    createTable: function (selector, viewName, options) {
        if (!viewName) {viewName = this._initialView;}
        if (!options) {options = this._options;}
        this._containerDiv = selector;
        var objPtr = this;
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display"></table>' );
        if (viewName == "allDocs") {
            WMStats.Couch.allDocs(options, function (overviewData) {
                objPtr._getLatestRequestIDsAndCreateTable(overviewData, objPtr)
            });
        } else {
            WMStats.Couch.view(viewName, options,  function (overviewData) {
                objPtr._getLatestRequestIDsAndCreateTable(overviewData, objPtr)
            });
        }
    }
};

(function($){
    if (WMStats.Globals.VARIANT == "tier1") {
        WMStats.Globals.loadScript("js/T1/WMStats.T1.RequestView.js", 
                            function() {WMStats.RequestView = WMStats.T1.RequestView;})
    } else if (WMStats.Globals.VARIANT == "tier0") {
        WMStats.Globals.loadScript("js/T0/WMStats.T0.RequestView.js", 
                            function() {WMStats.RequestView =  WMStats.T0.RequestView;})
    } else if (WMStats.Globals.VARIANT == "analysis") {
        WMStats.Globals.loadScript("js/T1/WMStats.Analysis.RequestView.js", 
                            function() {WMStats.RequestView =  WMStats.Analysis.RequestView;})
    } 
})(jQuery)