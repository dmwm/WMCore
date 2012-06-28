WMStats.namespace("Overview");

 (function($){
    if (WMStats.Globals.VARIANT == "tier1") {
        WMStats.Globals.loadScript("js/Views/WMStats.CampaignView.js", 
                            function() {WMStats.Overview = WMStats.CampaignView;})
    } else if (WMStats.Globals.VARIANT == "tier0") {
        WMStats.Globals.loadScript("js/T0/WMStats.T0.RunView.js", 
                            function() {WMStats.Overview =  WMStats.T0.RunView;})
    } else if (WMStats.Globals.VARIANT == "analysis") {
        WMStats.Globals.loadScript("js/Views/WMStats.CampaignView.js", 
                            function() {WMStats.Overview =  WMStats.CampaignView;})
    } 
})(jQuery);