//register mapping
(function(vm){
    WMStats.CategorySummaryMap.add(vm.SiteCategory.name(), WMStats.SiteSummary);
    WMStats.CategoryTableMap.add(vm.SiteCategory.name(), WMStats.SiteSummaryTable);
    
    WMStats.CategoryTableMap.add(vm.RequestView.categoryName, 
                                {'progress': WMStats.ActiveRequestTable});
    WMStats.CategorySummaryMap.add(vm.RunCategory.name(), WMStats.RunSummary);
    WMStats.CategoryTableMap.add(vm.RunCategory.name(), WMStats.RunSummaryTable);
})(WMStats.ViewModel);