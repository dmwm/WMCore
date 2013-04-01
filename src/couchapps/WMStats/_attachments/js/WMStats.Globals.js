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
            return "/an_reqmgr/view/details/";
        } else {
            return null;
        }
        
    };
    
    function getAlertCollectorLink() {
        return "/couchdb/alertscollector/_design/AlertsCollector/index.html"
    };
    
    function getWorkloadSummaryPrefix () {
        return "/couchdb/" + getWorkloadSummaryDB() + "/_design/WorkloadSummary/_show/histogramByWorkflow/";
    };
    
    function getWorkloadSummaryDB() {
        if (_dbVariants[dbname] == "tier1") {
            return "workloadsummary";
        } else if (_dbVariants[dbname] == "analysis") {
            return "analysis_workloadsummary";
        } else if (_dbVariants[dbname] == "tier0") {
            return "t0_workloadsummary";
        }
    };
    
    function getAgentUrlForJobs(agentURL, workflow, status) {
        return "http://" + agentURL.split(':')[0] + ":5984/wmagent_jobdump%2Fjobs/_design/JobDump/_list/" + 
                status + "Jobs/statusByWorkflowName?startkey=[%22" +
                workflow + "%22]&endkey=[%22" + workflow + "%22%2C{}]&reduce=false&stale=ok";
    }
    
    function formatJobLink(jobNumber, agentURLs, workflow, status) {
            if (jobNumber !== 0) {
                if (agentURLs.constructor.name === "String"){
                    agentURL = agentURLs
                } else if (agentURLs.length && (agentURLs[0].constructor.name === "String")){
                    //TODO: need to handle properly multiple agent
                    agentURL = agentURLs[0];
                } else {
                    return jobNumber
                }
                return "<a href='" + getAgentUrlForJobs(agentURL, workflow, status) +
                        "' target='_blank'>" + jobNumber + "</a>";
                                     
            } else {
                return jobNumber;
            };
    };

    return {
        REQ_DETAIL_URL_PREFIX: getReqDetailPrefix(),
        WORKLOAD_SUMMARY_URL_PREFIX: getWorkloadSummaryPrefix(),
        AJAX_LOADING_STATUS: {beforeSend: function(){$('#loading_page').addClass('front').show()}, 
                              complete: function(){$('#loading_page').hide()}},
        COUCHDB_NAME: dbname,
        WORKLOAD_SUMMARY_COUCHDB_NAME:  getWorkloadSummaryDB(), 
        REQMGR_COUCHDB_NAME: "reqmgr_workload_cache", //TODO: need to be configurable"reqmgrdb"
        VARIANT: _dbVariants[dbname],
        COUCHAPP_DESIGN: "WMStats",
        WORKLOAD_SUMMARY_COUCHAPP_DESIGN: "WorkloadSummary",
        REQMGR_COUCHAPP_DESIGN: "ReqMgr",
        ALERT_COLLECTOR_LINK: getAlertCollectorLink(),
        CONFIG: null, //this will be set when WMStats.Couch.loadConfig is called. just place holder or have default config
        loadScript: function (url, success) {
                        $.ajax({async: false, url: url, dataType: 'script', success: success})
            },
        importScripts: function (scripts) {
                        for (var i=0; i < scripts.length; i++) {
                                document.write('<script src="'+scripts[i]+'"><\/script>')
                                 }
              },
        Event: {}, // name space for Global Custom event
        formatJobLink: formatJobLink
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

WMStats.CustomEvents.LOADING_DIV_START = "C_9";
WMStats.CustomEvents.LOADING_DIV_END = "C_10";

WMStats.CustomEvents.HISTORY_LOADED = "C_11";
WMStats.CustomEvents.AJAX_LOADING_START = "C_12";
WMStats.CustomEvents.RESUBMISSION_SUMMARY_READY = "C_13";
WMStats.CustomEvents.RESUBMISSION_SUCCESS = "C_14";

//workload summary page event
WMStats.CustomEvents.WORKLOAD_SUMMARY_READY = "W_1";

//view model (need to move to proper location)
WMStats.namespace("ViewModel");
WMStats.ViewModel.Resubmission = {};
