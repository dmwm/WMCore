WMStats.namespace("RequestView")

WMStats.RequestView = (function() {
    
    var _data = null;
    var _containerDiv = null;
    var _initialView = 'requestByCampaignAndDate';
    var _options = {'include_docs': true};
    var _tableID = "requestTable";
    
    function _getOrDefault(baseObj, objList, val) {
        
        if (baseObj[objList[0]]) { 
            if (objList.length == 1) {
                return baseObj[objList[0]];
            } else {
                return _getOrDefault(baseObj[objList[0]], objList.slice(1), val);
            }
        } else {
            return val;
        } 
    }
    
    function _get(baseObj, objStr, val) {
        objList = objStr.split('.');
        return _getOrDefault(baseObj, objList, val); 
    }
    
    function formatReqDetailUrl(request) {
        return '<a href="' + WMStats.Globals.REQ_DETAIL_URL_PREFIX + encodeURIComponent(request) + '" target="_blank">' + request + '</a>';
    }
    
    function formatWorkloadSummarylUrl(request, status) {
        if (status == "completed" || status == "announced" ||
            status == "closed-out" || status == "deleted") {
            return '<a href="' + WMStats.Globals.WORKLOAD_SUMMARY_URL_PREFIX + encodeURIComponent(request) + '" target="_blank">' + status + '</a>';
        } else {
            return status;
        }
    }
    
    var tableConfig = {
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
    
    var filterConfig = {
        "aoColumns": [
            {type: "text", bRegex: true, bSmart: true},               
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true}
        ]
    }
    
    function getData() {
        return _data;
    }
    
    var keysFromIDs = function(data) {
        var keys = [];
        for (var i in data.rows){
            keys.push(data.rows[i].value.id);
        }
        return keys;      
    }

    var getRequestDetailsAndCreateTable = function (agentIDs, reqmgrData) {
        var options = {'keys': keysFromIDs(agentIDs), 'reduce': false, 
                       'include_docs': true};
        
        WMStats.Couch.allDocs(options,
              function(agentData) {
                  // combine reqmgrData(reqmgr_request) and 
                  // agent_request(agentData) data 
                  var requestCache = WMStats.Requests()
                  requestCache.updateBulkRequests(reqmgrData.rows)
                  requestCache.updateBulkRequests(agentData.rows)
                  
                  // set the data cache
                  _data = requestCache.getList();
                  
                  //create table
                  tableConfig.aaData = _data;
                  var selector = _containerDiv + " table";
                  return WMStats.Table(tableConfig).create(selector, filterConfig)
              })
    }
    
    var getLatestRequestIDsAndCreateTable = function (reqmgrData) {
        /*
         * get list of request ids first from the couchDB then 
         * get the details of the requests.
         */
    
        var options = {"keys": keysFromIDs(reqmgrData), "reduce": true, 
                       "group": true, "descending": true};
        WMStats.Couch.view('latestRequest', options,
              function(agentIDs) {
                  getRequestDetailsAndCreateTable(agentIDs, reqmgrData)
              })
    }
    
    
    function createTable(selector, viewName, options) {
        if (!viewName) {viewName = _initialView;}
        if (!options) {options = _options;}
        _containerDiv = selector;
        
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display"></table>' );
        if (viewName == "allDocs") {
            WMStats.Couch.allDocs(options, getLatestRequestIDsAndCreateTable)
        } else {
            WMStats.Couch.view(viewName, options, getLatestRequestIDsAndCreateTable);
        }
    }
    
    return {'getData': getData, 'createTable': createTable};
    
     
})();
    