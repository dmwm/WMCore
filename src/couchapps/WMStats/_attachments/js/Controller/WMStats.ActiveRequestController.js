WMStats.namespace("ActiveRequestController");
WMStats.ActiveRequestController.CategoryData = null;

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

(function($){
    
    var E = WMStats.CustomEvents;
    // Rewqest view filter event handler
    
    function drawFilteredRequestSummary() {
        var filteredData = getActiveFilteredData(true);
        WMStats.RequestDataList(filteredData.getSummary(), "#filter_summary");
    };
    
    function drawDataBoard() {
        
        var category = WMStats.Controls.getCategoryButtonValue();
        
        if (category == WMStats.Controls.requests) {
            var viewSelector = "#request_view";
            
        } else {
            var viewSelector = "#category_view";
        }
        
        var divSelector = viewSelector + " div.summary_data";
        $(divSelector).empty();
        var categoryData = getCategorizedData(category);
        // extend to other view type 
        var view = WMStats.CategoryTableMap.get(category);
        view(categoryData, divSelector);
        WMStats.GenericController.switchView(viewSelector)
        
    }
    
    $(WMStats.Globals.Event).on(E.REQUESTS_LOADED, 
        function(event, requestData) {
            //refresh filter cache.
            getActiveFilteredData();
            drawDataBoard();
            drawFilteredRequestSummary();
        })

    $(WMStats.Globals.Event).on(E.AGENTS_LOADED, 
        function(event, agentData) {
            //refresh filter cache.
            WMStats.AgentStatusGUI(agentData, "#message_board");
        })
    
    $(WMStats.Globals.Event).on(E.CATEGORY_SUMMARY_READY, 
        function(event, data) {
            var category = WMStats.Controls.getCategoryButtonValue();
            var categoryData = getCategorizedData(category);
            // extend to other view type 
            var view = WMStats.CategoryTableMap.get(category);
            view(categoryData, "#category_view div.summary_data");
            WMStats.GenericController.switchView("#category_view");
            WMStats.ActiveRequestController.CategoryData = categoryData;
        })
    
    $(WMStats.Globals.Event).on(E.REQUEST_SUMMARY_READY, 
        function(event, data) {
            //refresh filter cache.
            WMStats.ActiveRequestTable(data, "#request_view div.summary_data");
            WMStats.GenericController.switchView("#request_view");
        })

    $(WMStats.Globals.Event).on(E.JOB_SUMMARY_READY, 
        function(event, data) {
            WMStats.JobSummaryTable(data, "#job_view div.summary_data");
            WMStats.GenericController.switchView("#job_view");
        })

    $(WMStats.Globals.Event).on(E.CATEGORY_DETAIL_READY, 
        function(event, categoryKey) {
            var allData = WMStats.ActiveRequestController.CategoryData;
            var data = allData.getData(categoryKey);
            WMStats.RequestDetailList(data, "#category_view div.detail_data");
        })
        
    $(WMStats.Globals.Event).on(E.REQUEST_DETAIL_READY, 
        function(event, workflow) {
            var allRequests = WMStats.ActiveRequestModel.getRequests();
            var reqDoc = allRequests.getDataByWorkflow(workflow);
            var reqSummary = allRequests.getSummary(workflow);
            var data = {requests: reqDoc, summary: reqSummary};
        
            WMStats.RequestDetailList(data, "#request_view div.detail_data");
        })

    $(WMStats.Globals.Event).on(E.JOB_DETAIL_READY, 
        function(event, data) {
            WMStats.JobDetailList(data, "#job_view div.detail_data");
        })
        
    $(document).on('keyup', "#filter_board input", 
        function() {
            //refresh filter cache.
            getActiveFilteredData();
            drawDataBoard();
            drawFilteredRequestSummary();
        })

    $(document).on('change', 'input[name="category-select"][type="radio"]', function() {
        drawDataBoard();
        })

})(jQuery);
