WMStats.namespace("RequestView");

WMStats.RequestView = (function() {
    
    var _data = null;
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
                  return WMStats.Table(tableConfig).create(selector, filterConfig);
              })
    }
    
    var getLatestRequestIDsAndCreateTable = function (overviewData) {
        /*
         * get list of request ids first from the couchDB then 
         * get the details of the requests.
         */
        var options = {"keys": keysFromIDs(overviewData), "reduce": true, 
                       "group": true, "descending": true};
        WMStats.Couch.view('latestRequest', options,
              function(agentIDs) {
                  getRequestDetailsAndCreateTable(agentIDs, overviewData)
              })
    }
    
    
    function createTable(selector, viewName, options) {
        if (!viewName) {viewName = WMStats.RequestTable.initView;}
        if (!options) {options = WMStats.RequestTable.initOptions;}
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
    