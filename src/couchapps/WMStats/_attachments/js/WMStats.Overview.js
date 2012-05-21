WMStats.namespace("Overview")

WMStats.Overview = function() {
    function createTable(selector){
        if (WMStats.Globals.VARIANT == "tier1") {
            WMStats.CampaignView.createTable(selector);
        } else if (WMStats.Globals.VARIANT == "tier0") {
            WMStats.T0.RunView.createTable(selector);
        } else if (WMStats.Globals.VARIANT == "analysis") {
            WMStats.Analysis.RunView.createTable(selector);
        } 
    }
    return {createTable: createTable}
}()
