//import common scripts

WMStats.Globals.importScripts([
        "js/GUI/Tables/WMStats.Table.js",
        "js/GUI/Tables/WMStats.DefaultRequestTable.js",
        "js/GUI/Tables/WMStats.AgentTable.js",
        "js/GUI/Tables/WMStats.SiteTable.js",
        "js/GUI/Tables/WMStats.JobSummaryTable.js",
        "js/GUI/Tables/WMStats.CampaignTable.js",
        "js/GUI/Tables/WMStats.AlertTable.js",
        "js/GUI/Tables/WMStats.TableEventHandler.js"
    ])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/GUI/Tables/WMStats.ActiveRequestTable.js"])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/GUI/Tables/T0/WMStats.ActiveRequestTable.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/GUI/Tables/WMStats.ActiveRequestTable.js"])
} 
