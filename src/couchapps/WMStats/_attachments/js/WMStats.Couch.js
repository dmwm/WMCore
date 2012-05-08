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
    
    var _couchDB = $.couch.db(_dbName);
    
    function _combineOption(options, callback, ajaxOptions) {
        //combine options and callbacks for jquery.couch.js
        // ajaxOptions are object which contains jquery ajax options
        // {'beforeSend': .., 'complete': ...}
        var options = options || {};
        options.success = callback;
        var ajaxOptions = ajaxOptions || WMStats.Globals.AJAX_LOADING_STATUS
        if (ajaxOptions) {
            for (var opt in ajaxOptions) {
                options[opt] = ajaxOptions[opt];
            }
        }
        return options
    }
    
    function view(name, options, callback, ajaxOptions){
        //make all the view stale options update_after
        var options = options || {};
        if (options.stale != undefined) {
                options.stale = "update_after"
        }    
        return _couchDB.view(_Design +"/" + name, 
                             _combineOption(options, callback, ajaxOptions));
    }
    
    function allDocs(options, callback, ajaxOptions){
        return _couchDB.allDocs(_combineOption(options, callback, ajaxOptions));
    }
    
    return {'view': view, "allDocs": allDocs};
})()
