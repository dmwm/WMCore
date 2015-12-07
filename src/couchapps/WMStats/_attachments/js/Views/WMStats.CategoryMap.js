WMStats.namespace("CategorySummaryMap");
WMStats.namespace("CategoryTableMap");

WMStats.CategorySummaryMap = function(){
    var summaryMap = {};
    function add(category, summaryFunc) {
        summaryMap[category] = summaryFunc;
    }
    function get(category) {
        return summaryMap[category];
    }
    return {add: add, get: get};
}();

WMStats.CategoryTableMap = function(){
    var tableMap = {};
    var vm = WMStats.ViewModel;
    function add(category, view) {
        tableMap[category] = view;
    }
    function get(category, view) {
        if (category === vm.RequestView.categoryName) {
            return tableMap[category][vm.RequestView.format().name()];
        }else {
            return tableMap[category];
        }
        
    }
    return {add: add, get: get};
}();

(function(vm){
    //WMStats.CategoryTableMap.add(WMStats.Controls.requests, WMStats.ActiveRequestTableWithJob);
    
    // add controller
    vm.CategoryView.subscribe("data", function() {
        var view = WMStats.CategoryTableMap.get(vm.CategoryView.category().name());
        view(vm.CategoryView.data(), vm.CategoryView.category().id());
    });
    
    vm.RequestView.subscribe("data", function() {
        var view = WMStats.CategoryTableMap.get(vm.RequestView.categoryName);
        view(vm.RequestView.data(), vm.RequestView.format().id());
    });
})(WMStats.ViewModel);
