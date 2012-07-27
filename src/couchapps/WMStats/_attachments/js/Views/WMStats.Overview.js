WMStats.namespace("Overview");

 (function($){
    if (WMStats.Globals.VARIANT == "tier1") {
        WMStats.Globals.loadScript("js/Views/WMStats.CampaignView.js", 
                            function() {WMStats.Overview = WMStats.CampaignView;})
    } else if (WMStats.Globals.VARIANT == "tier0") {
        WMStats.Globals.loadScript("js/Views/WMStats.CampaignView.js", 
                            function() {WMStats.Overview =  WMStats.CampaignView;})
    } else if (WMStats.Globals.VARIANT == "analysis") {
        WMStats.Globals.loadScript("js/Views/WMStats.CampaignView.js", 
                            function() {WMStats.Overview =  WMStats.CampaignView;})
    } 
})(jQuery);