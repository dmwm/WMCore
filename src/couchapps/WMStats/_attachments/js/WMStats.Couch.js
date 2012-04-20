/*
 * Define Global values for couch db and couch db function
 * this has dependency on jquery.js and jquery.couch.js
 */
WMStats.namespace("Couch")

WMStats.Couch = (function(){
    // couchdb name for central summary db
    var _dbName = "wmstats";
    // couchapp name
    var _Design = "WMStats";
    var _reqDetailPrefix = "/reqmgr/view/details/";
    // this will depends on the variation of deployment
    var _workloadSummaryPrefix = "/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/";
    
    var _couchDB = $.couch.db(_dbName);
    
    function _combineOption(options, callback) {
        //combine options and callbacks for jquery.couch.js
        //TODO need to extend not just for success. (i.e failed case)
        var options = options || {};
        options.success = callback;
        return options
    }
    
    function view(name, options, callback){
        //make all the view stale options update_after
        var options = options || {};
        if (options.stale != undefined) {
                options.stale = "update_after"
        }    
        return _couchDB.view(_Design +"/" + name, 
                             _combineOption(options, callback));
    }
    
    function allDocs(options, callback){
        return _couchDB.allDocs(_combineOption(options, callback));
    }
    
    return {'view': view, "allDocs": allDocs, 
            "REQ_DETAIL_URL_PREFIX": _reqDetailPrefix,
            "WORKLOAD_SUMMARY_URL_PREFIX": _workloadSummaryPrefix};
})()
