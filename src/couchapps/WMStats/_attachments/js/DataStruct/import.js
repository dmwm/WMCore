WMStats.Globals.importScripts([
    "js/DataStruct/WMStats._StructBase.js",
    "js/DataStruct/WMStats.GenericRequests.js",
    "js/DataStruct/WMStats.Tasks.js",
    "js/DataStruct/WMStats.Agents.js",
    "js/DataStruct/WMStats.Sites.js",
    "js/DataStruct/WMStats.JobSummary.js",
    "js/DataStruct/WMStats.Campaigns.js",
    "js/DataStruct/WMStats.Alerts.js",
    "js/DataStruct/WMStats.SiteSummary.js",
    "js/DataStruct/WMStats.JobDetails.js",
    "js/DataStruct/WMStats.WorkloadSummary.js",
    "js/DataStruct/WMStats.History.js",
    "js/DataStruct/WMStats.LogDBData.js",
    "js/DataStruct/WMStats.LogMessage.js"
]);

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/DataStruct/T1/WMStats.RequestSummary.js",
                                   "js/DataStruct/T1/WMStats.CampaignSummary.js",
                                   "js/DataStruct/T1/WMStats.CMSSWSummary.js",
                                   "js/DataStruct/T1/WMStats.AgentRequestSummary.js",
                                   "js/DataStruct/T1/WMStats.ReqMgrRequest.js"]);
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/DataStruct/T0/WMStats.RequestSummary.js",
                                   "js/DataStruct/T0/WMStats.RunSummary.js"]);
}
