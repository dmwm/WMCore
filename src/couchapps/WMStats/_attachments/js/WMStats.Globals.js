/*
 * Define Global values in WMStats Application
 * TODO: This will contain configuration result for Tier0, Tier1, 
 * PromptScheme specific config
 */
WMStats.namespace("Globals");

WMStats.Globals = function($){
    var _dbVariants = {'wmstats': 'tier1', 'tier0_wmstats': 'tier0'};

	var reqPropertyMap = {
		   "_id": "_id",
		   "InputDataset": "inputdataset",
		   "PrepID": "prep_id",
		   "Group": "group",
		   "RequestDate": "request_date",
		   "Campaign": "campaign",
		   "RequestName": "workflow",
		   "RequestorDN": "user_dn",
		   "RequestPriority": "priority",
		   "Requestor": "requestor",
		   "RequestType": "request_type",
		   "DbsUrl": "dbs_url",
		   "CMSSWVersion": "cmssw",
		   "OutputDatasets": "outputdatasets",
		   "RequestTransition": "request_status", // Status: status,  UpdateTime: update_time
		   "SiteWhitelist": "site_white_list",
		   "Team": "team",
		   "TotalEstimatedJobs": "total_jobs",
		   "TotalInputEvents": "input_events",
		   "TotalInputLumis": "input_lumis",
		   "TotalInputFiles": "input_num_files",
		   "Run": "run",
		   "AgentJobInfo": "AgentJobInfo"
		};

    function convertRequestDocToWMStatsFormat(doc) {
    	// check document type whether it is reqmgr doc
    	// this is hacky way to check  - need better checking 
    	if (doc.RequestName == undefined) {
    		return doc;
    	};
    	
    	// this is temporary hack to identify missing RequestTransition property
    	// all the workflows which has missing info need to be manually updated
    	if (doc.RequestTransition == undefined || doc.RequestTransition.length == 0) {
    		doc.RequestTransition = [{"Status": "N/A", "UpdateTime": 0}];
    	}
    	var wmstatsReq = {};
    	for (var key in doc) {
    		if (reqPropertyMap[key]) {
    			if (key == "RequestTransition") {
    				wmstatsReq[reqPropertyMap[key]] = [];
    				for (var index = 0; index < doc[key].length; index++) {
    					wmstatsReq[reqPropertyMap[key]][index] = {"status": doc[key][index]["Status"], 
    															  "update_time": doc[key][index]["UpdateTime"]};
    				}
    			} else {
    				wmstatsReq[reqPropertyMap[key]] = doc[key];
    			}	
    		} else {
    			// use original key
    			wmstatsReq[key] = doc[key];
    		}
    			
    	}
    	return wmstatsReq;
    };
    
    function getReqDetailPrefix () {
        if (_dbVariants[dbname] == "tier1") {
            return "/reqmgr2/fetch?rid=";
        } else {
            return null;
        }
        
    };

    function getAlertCollectorLink() {
        return "/couchdb/alertscollector/_design/AlertsCollector/index.html";
    };
    
    function getWorkloadSummaryPrefix () {
        return "/couchdb/" + getWorkloadSummaryDB() + "/_design/WorkloadSummary/_show/histogramByWorkflow/";
    };
    
    function getGQLink(request) {
    	var gqLink = "/couchdb/workqueue/_design/WorkQueue/_rewrite/elementsInfo?request=" + request;
    	return "<a href='" + gqLink + "' target='_blank'>GQ</a>";
    };
    
    function getLQLink(agentURLs, request){
    	if (agentURLs.constructor.name === "String"){
            agentURL = agentURLs;
        } else if (agentURLs.length && (agentURLs[0].constructor.name === "String")){
            //TODO: need to handle properly multiple agent
            agentURL = agentURLs[0];
        };
    	var lqLink = "http://" + agentURL.split(':')[0] + 
    	             ":5984/workqueue/_design/WorkQueue/_rewrite/elementsInfo?request=" + request;
    	return "<a href='" + lqLink + "' target='_blank'>LQ</a>";
    };
    
    function getWorkloadSummaryDB() {
        if (_dbVariants[dbname] == "tier1") {
            return "workloadsummary";
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
                    agentURL = agentURLs;
                } else if (agentURLs.length && (agentURLs[0].constructor.name === "String")){
                    //TODO: need to handle properly multiple agent
                    agentURL = agentURLs[0];
                } else {
                    return jobNumber;
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
        AJAX_LOADING_STATUS: {beforeSend: function(){$('#loading_page').addClass('front').show();}, 
                              complete: function(){$('#loading_page').hide();}},
        COUCHDB_NAME: dbname,
        WORKLOAD_SUMMARY_COUCHDB_NAME:  getWorkloadSummaryDB(), 
        REQMGR_COUCHDB_NAME: "reqmgr_workload_cache", //TODO: need to be configurable"reqmgrdb"
        VARIANT: _dbVariants[dbname],
        COUCHAPP_DESIGN: "WMStats",
        WORKLOAD_SUMMARY_COUCHAPP_DESIGN: "WorkloadSummary",
		REQMGR_COUCHAPP_DESIGN: "ReqMgr",
        ALERT_COLLECTOR_LINK: getAlertCollectorLink(),
        T0_COUCHAPP_DESIGN: "T0Request",
        T0_COUCHDB_NAME: "t0_request",
        LOGDB_DESIGN: "LogDB",
        LOGDB_NAME: "wmstats_logdb",
        T0_LOGDB_NAME: "t0_logdb",
        CONFIG: null, //this will be set when WMStats.Couch.loadConfig is called. just place holder or have default config
        loadScript: function (url, success) {
                        $.ajax({async: false, url: url, dataType: 'script', success: success});
            },
        importScripts: function (scripts) {
                        for (var i=0; i < scripts.length; i++) {
                                document.write('<script src="'+scripts[i]+'"><\/script>');
                                 }
              },
        Event: {}, // name space for Global Custom event
        formatJobLink: formatJobLink,
        getGQLink: getGQLink,
        getLQLink: getLQLink,
        convertRequestDocToWMStatsFormat: convertRequestDocToWMStatsFormat
       };
}(jQuery);

WMStats.namespace("CustomEvents");

WMStats.CustomEvents.REQUESTS_LOADED = "C_1";
WMStats.CustomEvents.AGENTS_LOADED = "C_2";
//logDB list page event
WMStats.CustomEvents.LOG_LOADED = "L_1";
WMStats.CustomEvents.ERROR_LOG_LOADED = "L_2";

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
