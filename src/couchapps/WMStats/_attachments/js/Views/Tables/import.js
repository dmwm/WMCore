//import common scripts

WMStats.Globals.importScripts([
        "js/Views/Tables/WMStats.Table.js",
        "js/Views/Tables/WMStats.JobSummaryTable.js",
        "js/Views/Tables/WMStats.WorkloadSummaryTable.js"
    ])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T1/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T1/WMStats.CampaignSummaryTable.js",
                                   "js/Views/Tables/WMStats.SiteSummaryTable.js"])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T0/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T0/WMStats.RunSummaryTable.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T1/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T1/WMStats.CampaignSummaryTable.js",
                                   "js/Views/Tables/Analysis/WMStats.UserSummaryTable.js",
                                    "js/Views/Tables/WMStats.SiteSummaryTable.js"])
};
