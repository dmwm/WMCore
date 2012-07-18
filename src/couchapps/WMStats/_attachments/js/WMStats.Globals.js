/*
 * Define Global values in WMStats Application
 * TODO: This will contain configuration result for Tier0, Tier1, 
 * PromptScheme specific config
 */
WMStats.namespace("Globals")

WMStats.Globals = function($){
    var _dbVariants = {'wmstats': 'tier1', 'tier0_wmstats': 'tier0', 'analysis_wmstats': 'analysis' }
    return {
        REQ_DETAIL_URL_PREFIX: "/reqmgr/view/details/",
        WORKLOAD_SUMMARY_URL_PREFIX: "/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/",
        AJAX_LOADING_STATUS: {beforeSend: function(){$('#loading_page').show().addClass('front')}, 
                              complete: function(){$('#loading_page').hide()}},
        COUCHDB_NAME: dbname,
        VARIANT: _dbVariants[dbname],
        COUCHAPP_DESIGN: "WMStats",
        CONFIG: null, //this will be set when WMStats.Couch.loadConfig is called. just place holder or have default config
        loadScript: function (url, success) {
                        $.ajax({async: false, url: url, dataType: 'script', success: success})
            },
        importScripts: function (scripts) {
                        for (var i=0; i < scripts.length; i++) {
                                document.write('<script src="'+scripts[i]+'"><\/script>')
                                 }
                      }
        }
}(jQuery)
