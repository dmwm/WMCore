WMStats.namespace("ActiveRequestController");
WMStats.ActiveRequestController.CategoryData = null;

(function($){
    // Rewqest view filter event handler
    function getActiveFilteredData(cacheFlag){
        var requestData = WMStats.ActiveRequestModel.getRequests();
        if (cacheFlag) {
            return requestData.getFilteredRequests();
        } else {
            var filter = WMStats.Controls.getFilter();
            requestData.setFilter(filter);
            var filteredData = requestData.filterRequests();
            return filteredData
        }
    }
    
    function getCategorizedData(category) {
        
        var filteredData = getActiveFilteredData(true);
        var categoryData = filteredData;
        
        if (category != WMStats.Controls.requests) {
            
            var summaryStruct = WMStats.CategorySummaryMap.get(category);
            categoryData = WMStats.RequestsByKey(category, summaryStruct);
            categoryData.categorize(filteredData);
        }
        WMStats.ActiveRequestController.CategoryData = categoryData;
        return categoryData;
    }
    function drawRequestSummary() {
        var filteredData = getActiveFilteredData(true);
        WMStats.RequestDataList(filteredData.getSummary(), "#tab-active-request div[name='requestData']");
    };
    
    function drawCategoryView() {
        
        var divSelector = "#tab-active-request > div[name='requestSummary']";
        
        $(divSelector).empty();
        var category = WMStats.Controls.getCategoryButtonValue();
        var categoryData = getCategorizedData(category);
        // extend to other view type 
        var view = WMStats.CategoryTableMap.get(category);
        view(categoryData, divSelector);
        
    }
    
    $(WMStats.Globals.Event).on('activeRequestReady', 
        function(event, requestData) {
            //refresh filter cache.
            getActiveFilteredData();
            drawCategoryView();
            drawRequestSummary();
        })

    $(WMStats.Globals.Event).on('agentDataReady', 
        function(event, agentData) {
            //refresh filter cache.
            WMStats.AgentStatusGUI(agentData, "div[name='agent-status']");
            WMStats.AgentTable(agentData, "#tab-agent div[name='agentSummary']");
        })
        
    $(document).on('keyup', "div[name='filterDiv'] input", 
        function() {
            //refresh filter cache.
            getActiveFilteredData();
            drawCategoryView();
            drawRequestSummary();
        })

    $(document).on('change', 'input[name="category-select"][type="radio"]', function() {
        drawCategoryView();
        })

})(jQuery);
