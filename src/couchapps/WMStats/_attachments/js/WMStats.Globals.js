/*
 * Define Global values in WMStats Application
 * TODO: This will contain configuration result for Tier0, Tier1, 
 * PromptScheme specific config
 */
WMStats.namespace("Globals")

WMStats.Globals = function($){
    var _dbVariants = {'wmstats': 'tier1', 'tier0_wmstats': 'tier0', 'analysis_wmstats': 'analysis' }

    function getReqDetailPrefix () {
        if (_dbVariants[dbname] == "tier1") {
            return "/reqmgr/view/details/";
        } else if (_dbVariants[dbname] == "analysis") {
            return "/am_reqmgr/view/details/";
        } else {
            return null;
        }
        
    };
    
    function getWorkloadSummaryPrefix () {
        if (_dbVariants[dbname] == "tier1") {
            return "/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/";
        } else if (_dbVariants[dbname] == "analysis") {
            return "/couchdb/analysis_workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/";
        } else {
            return null;
        }
        
    };
    
    return {
        REQ_DETAIL_URL_PREFIX: getReqDetailPrefix(),
        WORKLOAD_SUMMARY_URL_PREFIX: getWorkloadSummaryPrefix(),
        AJAX_LOADING_STATUS: {beforeSend: function(){$('#loading_page').addClass('front').show()}, 
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
              },
        Event: {} // name space for Global Custom event
        }
}(jQuery)

WMStats.namespace("CustomEvents");

WMStats.CustomEvents.REQUESTS_LOADED = "C_1";
WMStats.CustomEvents.AGENTS_LOADED = "C_2";


WMStats.CustomEvents.REQUEST_SUMMARY_READY = "C_3";
WMStats.CustomEvents.CATEGORY_SUMMARY_READY = "C_4";

WMStats.CustomEvents.REQUEST_DETAIL_READY = "C_5";
WMStats.CustomEvents.CATEGORY_DETAIL_READY = "C_6";

WMStats.CustomEvents.JOB_SUMMARY_READY = "C_7";
WMStats.CustomEvents.JOB_DETAIL_READY = "C_8";
