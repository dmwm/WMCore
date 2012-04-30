/*
 * Define Global values in WMStats Application
 * TODO: This will contain configuration result for Tier0, Tier1, 
 * PromptScheme specific config
 */
WMStats.namespace("Globals")
WMStats.Globals = {
    REQ_DETAIL_URL_PREFIX: "/reqmgr/view/details/",
    WORKLOAD_SUMMARY_URL_PREFIX: "/couchdb/workloadsummary/_design/WorkloadSummary/_show/histogramByWorkflow/",
    // set the flag to prevent sending ajax call by clicking table rows 
    RequestViewTableClickFlag: false
}
