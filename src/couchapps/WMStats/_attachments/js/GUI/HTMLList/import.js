WMStats.Globals.importScripts([
    //"js/GUI/HTMLList/WMStats.HTMLList.js",
    "js/GUI/HTMLList/WMStats.JobDetailList.js",
    "js/GUI/HTMLList/WMStats.RequestDataList.js"
])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/GUI/HTMLList/WMStats.RequestDetailList.js"])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/GUI/HTMLList/T0/WMStats.RequestDetailList.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/GUI/HTMLList/WMStats.RequestDetailList.js"])
} 
