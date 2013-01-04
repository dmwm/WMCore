WMStats.namespace("CategorySummaryMap");
WMStats.namespace("CategoryTableMap");

WMStats.CategorySummaryMap = function(){
    var summaryMap = {};
    function add(category, summaryFunc) {
        summaryMap[category] = summaryFunc;
    }
    function get(category) {
        return summaryMap[category]
    }
    return {add: add, get: get}
}()

WMStats.CategoryTableMap = function(){
    var tableMap = {};
    function add(category, view) {
        tableMap[category] = view;
    }
    function get(category, view) {
        return tableMap[category];
    }
    return {add: add, get: get}
}()

//register mapping
WMStats.CategorySummaryMap.add(WMStats.Controls.sites, WMStats.SiteSummary);
WMStats.CategoryTableMap.add(WMStats.Controls.sites, WMStats.SiteSummaryTable);

WMStats.CategoryTableMap.add(WMStats.Controls.requests, WMStats.ActiveRequestTable);
