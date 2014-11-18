//import common scripts

WMStats.Globals.importScripts([
        "js/Views/Tables/WMStats.Table.js",
        "js/Views/Tables/WMStats.JobSummaryTable.js",
        "js/Views/Tables/WMStats.WorkloadSummaryTable.js",
        "js/Views/Tables/WMStats.TableController.js"
    ]);

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T1/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T1/WMStats.ActiveRequestTableWithJob.js",
                                   "js/Views/Tables/T1/WMStats.TaskSummaryTable.js",
                                   "js/Views/Tables/T1/WMStats.CampaignSummaryTable.js",
                                   "js/Views/Tables/T1/WMStats.SiteSummaryTable.js",
                                   "js/Views/Tables/T1/WMStats.CMSSWSummaryTable.js",
                                   "js/Views/Tables/T1/WMStats.AgentRequestSummaryTable.js",
                                   "js/Views/Tables/T1/addCategoryMap.js"]);
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/Views/Tables/T0/WMStats.ActiveRequestTable.js",
                                   "js/Views/Tables/T0/WMStats.RunSummaryTable.js",
                                   "js/Views/Tables/T0/WMStats.TaskSummaryTable.js",
                                   "js/Views/Tables/T0/addCategoryMap.js"]);
};
