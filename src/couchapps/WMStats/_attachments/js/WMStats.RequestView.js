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
    
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow"},
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status"},
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset",
                           "sDefaultContent": ""},
            { "mDataProp": "site_white_list", "sTitle": "site white list",
                           "sDefaultContent": ""},
            { "mDataProp": "priority", "sTitle": "priority"},
            { "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.queued.first", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.pending", 0);
                          }
            },
            { "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            { "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.failure.create", 0) + 
                                    _get(o.aData, "status.failure.submit", 0) + 
                                    _get(o.aData, "status.failure.exception", 0));
                          }
            },
            
            { "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.canceled", 0);
                          }},
            { "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.success", 0);
                          }},
            { "sTitle": "cool off", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.cooloff", 0);
                          }
            },
            { "sTitle": "pre-cooloff",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.submitted.retry", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return (_get(o.aData, "status.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1));
                          }},
            
            //TODO add more data (consult dataops)
        ]
    }
    
    var filterConfig = {
        "aoColumns": [
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
                  var selector = _containerDiv + " table#" + _tableID;
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
        
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="'+ _tableID + '"></table>' );
        WMStats.Couch.view(viewName, options, getLatestRequestIDsAndCreateTable)
    }
    
    return {'getData': getData, 'createTable': createTable};
    
     
})();
    