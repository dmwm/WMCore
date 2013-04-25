//T1 specific categoryMap
(function(vm){
    //register mapping
    WMStats.CategorySummaryMap.add(vm.SiteCategory.name(), WMStats.SiteSummary);
    WMStats.CategoryTableMap.add(vm.SiteCategory.name(), WMStats.SiteSummaryTable);
    
    WMStats.CategoryTableMap.add(vm.RequestView.categoryName, 
                                {'progress': WMStats.ActiveRequestTable,
                                 'numJobs':WMStats.ActiveRequestTableWithJob});
                                 
    WMStats.CategorySummaryMap.add(vm.CampaignCategory.name(), WMStats.CampaignSummary);
    WMStats.CategoryTableMap.add(vm.CampaignCategory.name(), WMStats.CampaignSummaryTable);
    WMStats.CategorySummaryMap.add(vm.CMSSWCategory.name(), WMStats.CMSSWSummary);
    WMStats.CategoryTableMap.add(vm.CMSSWCategory.name(), WMStats.CMSSWSummaryTable);
    WMStats.CategorySummaryMap.add(vm.AgentCategory.name(), WMStats.AgentRequestSummary);
    WMStats.CategoryTableMap.add(vm.AgentCategory.name(), WMStats.AgentRequestSummaryTable);
})(WMStats.ViewModel);
