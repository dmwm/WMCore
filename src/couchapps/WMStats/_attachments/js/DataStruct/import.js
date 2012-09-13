WMStats.Globals.importScripts([
    "js/DataStruct/WMStats._StructBase.js",
    "js/DataStruct/WMStats.GenericRequests.js",
    "js/DataStruct/WMStats.Agents.js",
    "js/DataStruct/WMStats.Sites.js",
    "js/DataStruct/WMStats.JobSummary.js",
    "js/DataStruct/WMStats.Campaigns.js",
    "js/DataStruct/WMStats.Alerts.js",
    "js/DataStruct/WMStats.SiteSummary.js",
    "js/DataStruct/WMStats.JobDetails.js"
])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/DataStruct/T1/WMStats.Requests.js",
                                   "js/DataStruct/T1/WMStats.CampaignSummary.js"])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/DataStruct/T0/WMStats.Requests.js",
                                   "js/DataStruct/T0/WMStats.RunSummary.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/DataStruct/T1/WMStats.Requests.js"])
} 
