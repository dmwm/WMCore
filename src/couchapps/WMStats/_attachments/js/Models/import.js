WMStats.Globals.importScripts([
    "js/Models/WMStats._ModelBase.js",
    "js/Models/WMStats._RequestModelBase.js",
    "js/Models/WMStats.JobSummaryModel.js",
    "js/Models/WMStats.JobDetailModel.js",
    "js/Models/WMStats.AgentModel.js",
])

if (WMStats.Globals.VARIANT == "tier1") {
    WMStats.Globals.importScripts(["js/Models/T1/WMStats.ActiveRequestModel.js"]);
} else if (WMStats.Globals.VARIANT == "tier0") {
    WMStats.Globals.importScripts(["js/Models/T0/WMStats.ActiveRequestModel.js"]);
} else if (WMStats.Globals.VARIANT == "analysis") {
    WMStats.Globals.importScripts(["js/Models/T1/WMStats.ActiveRequestModel.js"]);
} 