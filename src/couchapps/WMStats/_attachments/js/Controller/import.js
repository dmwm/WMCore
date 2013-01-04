WMStats.Globals.importScripts([
    "js/Controller/WMStats.Env.js",
    "js/Controller/WMStats.GenericController.js",
    "js/Controller/WMStats.ActiveRequestController.js",
    "js/Controller/WMStats.CategoryMap.js",
    "js/Controller/WMStats.TableController.js"
])

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts([
        "js/Controller/T1/addCategoryMap.js",
    ])
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts(["js/Controller/T0/addCategoryMap.js"])
} else if (WMStats.Globals.VARIANT == "analysis") {
    //import analysis specific table
    WMStats.Globals.importScripts(["js/Controller/Analysis/addCategoryMap.js"])
};

