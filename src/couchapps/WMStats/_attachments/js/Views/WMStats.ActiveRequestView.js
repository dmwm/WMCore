WMStats.namespace("ActiveRequestView");

 (function($){
    if (WMStats.Globals.VARIANT == "tier1") {
        WMStats.Globals.loadScript("js/T1/WMStats.T1.ActiveRequestView.js", 
                            function() {WMStats.ActiveRequestView = WMStats.T1.ActiveRequestView;})
    } else if (WMStats.Globals.VARIANT == "tier0") {
        WMStats.Globals.loadScript("js/T0/WMStats.T0.ActiveRequestView.js", 
                            function() {WMStats.ActiveRequestView =  WMStats.T0.ActiveRequestView;})
    } else if (WMStats.Globals.VARIANT == "analysis") {
        WMStats.Globals.loadScript("js/T1/WMStats.T1.ActiveRequestView.js", 
                            function() {WMStats.ActiveRequestView =  WMStats.T1.ActiveRequestView;})
    } 
})(jQuery);