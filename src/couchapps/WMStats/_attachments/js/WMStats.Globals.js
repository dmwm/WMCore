/*
 * Define Global values in WMStats Application
 * TODO: This will contain configuration result for Tier0, Tier1, 
 * PromptScheme specific config
 */
WMStats.namespace("Globals")

WMStats.Globals = function(){
    // get the couchdb name from the path
    var _couchDBName = "";
    var _dbVariants = {'wmstats': 'tier1', 'tier0_wmstats': 'tier0', 'analysis_wmstats': 'analysis' }
    var pathList = window.location.pathname.split('/_design/WMStats');
    var tempList = pathList[0].split('/');
    if (pathList.length > 1) {
        _couchDBName = tempList[tempList.length - 1];
    } else {
        //this is hacky code relying on the rewrite rule in cms. couchdb/[couchdb_name]
        _couchDBName = tempList[1];
    }
    return {
        REQ_DETAIL_URL_PREFIX: "/reqmgr/view/details/",
        WORKLOAD_SUMMARY_URL_PREFIX: "/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/",
        AJAX_LOADING_STATUS: {beforeSend: function(){$('#loading_page').show().addClass('front')}, 
                              complete: function(){$('#loading_page').hide()}},
        COUCHDB_NAME: _couchDBName,
        VARIANT: _dbVariants[_couchDBName],
        COUCHAPP_DESIGN: "WMStats",
        CONFIG: null, //this will be set when WMStats.Couch.loadConfig is called. just place holder or have default config
        loadScript: function (url, success) {
                        $.ajax({async: false, url: url, dataType: 'script', success: success})
                    }
    }
}()
