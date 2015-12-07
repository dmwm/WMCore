/*
 * Define Global values for couch db and couch db function
 * this has dependency on jquery.js and jquery.couch.js
 */
WMStats.namespace("CouchBase");
WMStats.namespace("Couch");
WMStats.namespace("WorkloadSummaryCouch");
WMStats.namespace("ReqMgrCouch");

WMStats.CouchBase = function(dbName, designName){
    // couchapp name
    var _Design = designName;
    var _couchDB = $.couch.db(dbName);
    var _config;

    function _combineOption(options, callback, ajaxOptions) {
        //combine options and callbacks for jquery.couch.js
        // ajaxOptions are object which contains jquery ajax options
        // {'beforeSend': .., 'complete': ...}
        var options = options || {};
        options.success = callback;
        //var ajaxOptions = ajaxOptions || WMStats.Globals.AJAX_LOADING_STATUS
        //var ajaxOptions = ajaxOptions
        if (ajaxOptions) {
            for (var opt in ajaxOptions) {
                options[opt] = ajaxOptions[opt];
            }
        }
        return options;
    };

    function loadConfig(func) {
        function callback(data) {
            //fist set the global config value
            var config;
            if (!data.rows && (data.rows.length != 1)) {
                //use default config
                config = null;
            } else {
                config = data.rows[0].doc;
            }
            WMStats.Globals.CONFIG = config;
            // then call the function
            func();
        }
        _couchDB.view(_Design +"/config", 
                      _combineOption({"include_docs": true}, callback));
        
    }

    function view(name, options, callback, ajaxOptions){
        //make all the view stale options update_after
        var options = options || {};
        if (options.stale === undefined) {
            options.stale = "update_after";
        }
        return _couchDB.view(_Design +"/" + name, 
                             _combineOption(options, callback, ajaxOptions));
    }
    
    function allDocs(options, callback, ajaxOptions){
        return _couchDB.allDocs(_combineOption(options, callback, ajaxOptions));
    }
    
    function openDoc(docId, callback){
        return _couchDB.openDoc(docId, _combineOption({}, callback)); 
    }

    return {'loadConfig': loadConfig, 'view': view, "allDocs": allDocs,
            'openDoc': openDoc};
};

WMStats.Couch = WMStats.CouchBase(WMStats.Globals.COUCHDB_NAME, WMStats.Globals.COUCHAPP_DESIGN);
WMStats.WorkloadSummaryCouch = WMStats.CouchBase(WMStats.Globals.WORKLOAD_SUMMARY_COUCHDB_NAME,
                                                 WMStats.Globals.WORKLOAD_SUMMARY_COUCHAPP_DESIGN);
WMStats.ReqMgrCouch = WMStats.CouchBase(WMStats.Globals.REQMGR_COUCHDB_NAME,
                                                 WMStats.Globals.REQMGR_COUCHAPP_DESIGN);
WMStats.T0Couch = WMStats.CouchBase(WMStats.Globals.T0_COUCHDB_NAME,
                                                 WMStats.Globals.T0_COUCHAPP_DESIGN);
WMStats.LogDBCouch = WMStats.CouchBase(WMStats.Globals.LOGDB_NAME,
                                                 WMStats.Globals.LOGDB_DESIGN);
WMStats.T0LogDBCouch = WMStats.CouchBase(WMStats.Globals.T0_LOGDB_NAME,
                                                 WMStats.Globals.LOGDB_DESIGN);