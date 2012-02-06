WMStats.namespace("RequestView")

WMStats.RequestView = (function() {
    
    var _data = null;
    var _containerDiv = null;
    var _url = WMStats.Globals.couchDBViewPath + 'campaign-request';
    var _options = {'include_docs': true};
    var _tableID = "requestTable";
    function _get(obj, val) {
        if (obj) {
            return obj;
        } else {
            return val;
        } 
    }
    
    
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow"},
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset"},
            { "mDataProp": "site_white_list", "sTitle": "site white list"},
            //{ "mDataProp": "status.inWMBS", "sTitle": "in wmbs", 
            //               "sDefaultContent": 0, "bVisible": false},
            
            { "mDataProp": "status.queued.first", "sTitle": "queued first", 
                           "sDefaultContent": 0 , "bVisible": false},
            { "mDataProp": "status.queued.retry", "sTitle": "queued retry", 
                           "sDefaultContent": 0, "bVisible": false },
            { "sTitle": "queued", "sDefaultContent": 0, 
                        "fnRender": function ( o, val ) {
                                      return _get(o.aData.status.queued.first, 0) + 
                                             _get(o.aData.status.queued.retry, 0);
                                    }
            },
                           
            { "mDataProp": "status.submitted.first", "sTitle": "submitted first", 
                           "sDefaultContent": 0, "bVisible": false },
            { "mDataProp": "status.submitted.retry", "sTitle": "submitted retry", 
                           "sDefaultContent": 0, "bVisible": false },
            { "mDataProp": "status.submitted.pending", "sTitle": "pending", "sDefaultContent": 0 },
            { "mDataProp": "status.submitted.running", "sTitle": "running", "sDefaultContent": 0 },
            
            { "mDataProp": "status.failure.create", "sTitle": "create fail", 
                           "sDefaultContent": 0, "bVisible": false  },
            { "mDataProp": "status.failure.submit", "sTitle": "submit fail", 
                           "sDefaultContent": 0, "bVisible": false },
            { "mDataProp": "status.failure.exception", "sTitle": "exception fail", 
                           "sDefaultContent": 0, "bVisible": false },
            { "sTitle": "failure", "sDefaultContent": 0, 
                        "fnRender": function ( o, val ) {
                                      return (_get(o.aData.status.failure.create, 0) + 
                                              _get(o.aData.status.failure.submit, 0) + 
                                              _get(o.aData.status.failure.exception, 0));
                                    }
            },
            
            { "mDataProp": "status.canceled", "sTitle": "canceled", "sDefaultContent": 0 },
            { "mDataProp": "status.success", "sTitle": "success", "sDefaultContent": 0 },
            { "mDataProp": "status.cooloff", "sTitle": "cool off", "sDefaultContent": 0 },
            { "sTitle": "pre-cool offed", "sDefaultContent": 0, 
                        "fnRender": function ( o, val ) {
                                      return (_get(o.aData.status.submitted.retry, 0) + 
                                              _get(o.aData.status.queued.retry, 0));
                                    }
            },
            { "mDataProp": "total_jobs", "sTitle": "total estimate jobs", 
                           "sDefaultContent": 0, "bVisible": false},
            { "sTitle": "queue injection", "sDefaultContent": 0, 
                        "fnRender": function ( o, val ) {
                                        return _get(o.aData.status.inWMBS, 0) / _get(o.aData['total_jobs'], 1);
                                    }}
            //TODO add more data
        ]
    }

    function getData() {
        return _data;
    }
    
    var keysFromIDs = function(data) {
        var keys = [];
        for (var i in data.rows){
            keys.push(data.rows[i].id);
        }
        //TODO not sure why JSON.stringify cause the problem
        return keys;      
    }   
                
    var getRequestDetailsAndCreateTable = function (agentIDs, reqmgrData) {
        var options = {'keys': keysFromIDs(agentIDs), 'reduce': false, 'include_docs': true};
        //TODO need to change to post call
        var url = WMStats.Globals.couchDBViewPath + 'latest-request'
        $.get(url, options,
              function(agentData) {
                  //combine reqmgrData(reqmgr_request) and agent_request(agentData) data 
                  var requestCache = WMStats.Requests()
                  requestCache.updateBulkRequests(reqmgrData.rows)
                  requestCache.updateBulkRequests(agentData.rows)
                  
                  // set the data cache
                  _data = requestCache.getList();
                  
                  //create table
                  tableConfig.aaData = _data;
                  var selector = _containerDiv + " table#" + _tableID;
                  return WMStats.Table(tableConfig).create(selector)
              },
              'json')
    }
    
    var getLatestRequestIDsAndCreateTable = function (reqmgrData) {
        /*
         * get list of request ids first from the couchDB then get the details of the requests.
         * This is due to the reduce restiction on couchDB - can't be one http call. 
         */
    
        var options = {"keys": keysFromIDs(reqmgrData), "reduce": true, 
                       "group": true, "descending": true};
        //TODO need to change to post call
        var url = WMStats.Globals.couchDBViewPath + 'latest-request';
        $.get(url, options,
              function(agentIDs) {
                  getRequestDetailsAndCreateTable(agentIDs, reqmgrData)
              },
              'json')
    }
    
    
    function createTable(selector, url, options) {
        if (!url) {url = _url;}
        if (!options) {options = _options;}
        _containerDiv = selector;
        
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="'+ _tableID + '"></table>' );
        $.get(url, options, getLatestRequestIDsAndCreateTable, 'json')
    }
    
    return {'getData': getData, 'createTable': createTable};
    
     
})();
    