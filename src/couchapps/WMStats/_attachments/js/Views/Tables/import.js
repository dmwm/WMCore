//import common scripts

WMStats.Globals.importScripts([
        "js/Views/Tables/WMStats.Table.js",
        "js/Views/Tables/WMStats.DefaultRequestTable.js",
        "js/Views/Tables/WMStats.AgentTable.js",
        "js/Views/Tables/WMStats.SiteTable.js",
        "js/Views/Tables/WMStats.JobSummaryTable.js",
        "js/Views/Tables/WMStats.CampaignTable.js",
        "js/Views/Tables/WMStats.AlertTable.js",
        "js/Views/Tables/WMStats.SiteSummaryTable.js",
    ])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T1/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T1/WMStats.CampaignSummaryTable.js"])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T0/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T0/WMStats.RunSummaryTable.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T1/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T1/WMStats.CampaignSummaryTable.js"])
} 
