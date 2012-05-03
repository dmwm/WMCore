/*
 * Define Global values in WMStats Application
 * TODO: This will contain configuration result for Tier0, Tier1, 
 * PromptScheme specific config
 */
WMStats.namespace("Globals")
WMStats.Globals = function(){
    function _getCouchDB() {
        pathList = window.location.pathname.split('/_design/WMStats');
        if (pathList.length > 1) {
            // accessing through 
        } else {
        }
    return {
        REQ_DETAIL_URL_PREFIX: "/reqmgr/view/details/",
        WORKLOAD_SUMMARY_URL_PREFIX: "/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/",
        AJAX_LOADING_STATUS: {beforeSend: function(){$('#loading_page').show().addClass('front')}, 
                              complete: function(){$('#loading_page').hide()}},
        getCouchDB: _couchDB
    }
}()

WMStats.Globals