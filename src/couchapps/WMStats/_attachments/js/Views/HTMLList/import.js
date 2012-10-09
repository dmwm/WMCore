WMStats.Globals.importScripts([
    //"js/Views/HTMLList/WMStats.HTMLList.js",
    "js/Views/HTMLList/WMStats.JobDetailList.js",
    "js/Views/HTMLList/WMStats.AgentStatusGUI.js"
])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/Views/HTMLList/T1/WMStats.RequestDetailList.js",
                                   "js/Views/HTMLList/T1/WMStats.RequestAlertGUI.js",
                                   "js/Views/HTMLList/T1/WMStats.RequestDataList.js"])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/Views/HTMLList/T0/WMStats.RequestDetailList.js",
                                   "js/Views/HTMLList/T0/WMStats.RequestAlertGUI.js",
                                   "js/Views/HTMLList/T0/WMStats.RequestDataList.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/Views/HTMLList/Analysis/WMStats.RequestDetailList.js",
                                   "js/Views/HTMLList/T1/WMStats.RequestAlertGUI.js",
                                   "js/Views/HTMLList/T1/WMStats.RequestDataList.js"])
} 
