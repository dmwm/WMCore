//import common scripts
WMStats.Globals.importScripts(["js/Views/Controls/WMStats.CommonControls.js"]);

if (WMStats.Globals.VARIANT == "tier1") {
    //import tier1 specific table
    WMStats.Globals.importScripts(["js/Views/Controls/T1/WMStats.Controls.js"]);
} else if (WMStats.Globals.VARIANT == "tier0") {
    //import tie0 specific table
    WMStats.Globals.importScripts( ["js/Views/Controls/T0/WMStats.Controls.js"]);
};