WMStats.namespace("ActiveRequestController");

function getActiveFilteredData(cacheFlag){
    var requestData = WMStats.ActiveRequestModel.getRequests();
    if (cacheFlag) {
        return requestData.getFilteredRequests();
    } else {
        return filterRequests(requestData)
    }
}

function filterRequests(requestData) {
    var filter = WMStats.Controls.getFilter();
    requestData.setFilter(filter);
    var filteredData = requestData.filterRequests();
    return filteredData;
}

function getCategorizedData(category) {
    
    var categoryData;
    
    if (category != WMStats.Controls.requests) {
        var filteredData = getActiveFilteredData(true);
        var summaryStruct = WMStats.CategorySummaryMap.get(category);
        categoryData = WMStats.RequestsByKey(category, summaryStruct);
        categoryData.categorize(filteredData);
        WMStats.Env.CategoryData = categoryData;
    } else {
        categoryData = filterRequests(WMStats.Env.CurrentRequestData);
    }
    return categoryData;
}

(function($){
    
    var E = WMStats.CustomEvents;
    // Request view filter event handler
    function drawTotalRequestSummary() {
        var requestData = WMStats.ActiveRequestModel.getRequests();
        WMStats.RequestSummaryList(requestData.getSummary(), "#summary_board");
    };
    
    function drawFilteredRequestSummary() {
        var filteredData = getActiveFilteredData(true);
        WMStats.RequestSummaryList(filteredData.getSummary(), "#filter_summary");
    };

    function drawDataBoard(viewSelector) {
        if (!viewSelector) {
            viewSelector = WMStats.Env.View;
        }
        //find which view needs to draw.
        WMStats.GenericController.switchView(viewSelector);
        var divSelector = viewSelector + " div.summary_data";
        var category = null;
        if (viewSelector === "#category_view") {
            // get category
            category = WMStats.Env.CategorySelection
        } else if (viewSelector === "#request_view") {
            category = WMStats.Controls.requests;
        } // maybe needs job view as well
        
        if (category) {
            /*
            // clean databoard
            $(viewSelector + " div.detail_data").empty();
            $(divSelector).empty();
            */
            var data = getCategorizedData(category);
            var view = WMStats.CategoryTableMap.get(category);
            view(data, divSelector);
        }
    }
    
    $(WMStats.Globals.Event).on(E.REQUESTS_LOADED, 
        function(event, requestData) {
            var requestData = WMStats.ActiveRequestModel.getRequests();
            // draw alert
            WMStats.RequestAlertGUI(requestData, "#request_alert");
            drawTotalRequestSummary()
            //refresh filter cache.
            getActiveFilteredData();
            drawFilteredRequestSummary();
            
            // update CurrentRequestData only for the all_requests or initialize
            if (WMStats.Env.CurrentRequestData === null || 
                WMStats.Env.RequestSelection === "all_requests") {
                //WMStats.Env.CurrentRequestData = requestData.getFilteredRequests();
                WMStats.Env.CurrentRequestData = requestData;
            }
            drawDataBoard();
        })

    $(WMStats.Globals.Event).on(E.AGENTS_LOADED, 
        function(event, agentData) {
            //refresh filter cache.
            WMStats.AgentStatusGUI(agentData, "#agent_alert");
        })
    
    $(WMStats.Globals.Event).on(E.CATEGORY_SUMMARY_READY, 
        function(event, data) {
            $("#category_view div.detail_data").empty();
            drawDataBoard("#category_view");
        })
    
    $(WMStats.Globals.Event).on(E.REQUEST_SUMMARY_READY, 
        function(event, data) {
            $("#request_view div.detail_data").empty();
            if (data) {
                WMStats.Env.CurrentRequestData = data;
            }
            drawDataBoard("#request_view");
        })

    $(WMStats.Globals.Event).on(E.JOB_SUMMARY_READY, 
        function(event, data) {
            $("#job_view div.detail_data").empty();
            WMStats.JobSummaryTable(data, "#job_view div.summary_data");
            WMStats.GenericController.switchView("#job_view");
        })

    $(WMStats.Globals.Event).on(E.CATEGORY_DETAIL_READY, 
        function(event, categoryKey) {
            var allData = WMStats.Env.CategoryData;
            var data = allData.getData(categoryKey);
            WMStats.CategoryDetailList(data, "#category_view div.detail_data");
        })
        
    $(WMStats.Globals.Event).on(E.REQUEST_DETAIL_READY, 
        function(event, workflow) {
            var allRequests = WMStats.ActiveRequestModel.getRequests();
            var reqDoc = allRequests.getDataByWorkflow(workflow);
            var reqSummary = allRequests.getSummary(workflow);
            var requests = {};
            requests[workflow] = reqDoc;
            var data = {key: workflow, requests: requests, summary: reqSummary};
            //$("#request_view div.detail_data").show("slide", {direction: "down"}, 500);
            WMStats.RequestDetailList(data, "#request_view div.detail_data");
            $("#request_view div.detail_data").show("slide", {}, 500);
            WMStats.Env.RequestDetailOpen = true;
        })

    $(WMStats.Globals.Event).on(E.JOB_DETAIL_READY, 
        function(event, data) {
            WMStats.JobDetailList(data, "#job_view div.detail_data");
        })
        
    // filter control
    $(document).on('keyup', "#filter_board input", 
        function() {
            //refresh filter cache.
            getActiveFilteredData();
            drawFilteredRequestSummary();
            drawDataBoard();
        })
/*
    $(document).on('change', 'input[name="category-select"][type="radio"]', function() {
        drawDataBoard();
        })
*/
    $(document).on('click', "#category_button li a", function(event){
        WMStats.Env.CategorySelection = this.hash.substring(1);
        $(WMStats.Globals.Event).triggerHandler(E.CATEGORY_SUMMARY_READY);
        $("#category_button li a").removeClass("nav-button-selected").addClass("button-unselected");
        $(this).removeClass("button-unselected").addClass("nav-button-selected");
        event.preventDefault();
        })
     
    $(document).on('click', "#all_requests li a", function(event){
        WMStats.Env.RequestSelection = "all_requests";
        var data = WMStats.ActiveRequestModel.getRequests();
        $(WMStats.Globals.Event).triggerHandler(E.REQUEST_SUMMARY_READY, data);
        $(this).removeClass("button-unselected").addClass("nav-button-selected");
        event.preventDefault();
        })

     $(document).on('click', "#view_switch_button li a", function(event){
        WMStats.Env.ViewSwitchSelection = this.hash.substring(1);
        //TODO need to get the data
        $(WMStats.Globals.Event).triggerHandler(E.REQUEST_SUMMARY_READY);
        $(this).removeClass("button-unselected").addClass("nav-button-selected");
        event.preventDefault();
        })
        
    $(document).on('click', 'a.requestAlert', function() {
        var workflow = $(this).text();
        WMStats.JobSummaryModel.setRequest(workflow);
        $(WMStats.Globals.Event).triggerHandler(E.AJAX_LOADING_START)
        WMStats.JobSummaryModel.retrieveData();
        $(this).addClass('reviewed');
       })

    $(document).on('click', "#tab_board li a", function(event){
        drawDataBoard(this.hash);
        event.preventDefault();
    });
    
    $(document).on('click', "#jobDetailNav li a", function(event){
        $('div.jobDetailBox').hide();
        $(this.hash).show();
        $("#jobDetailNav li a").removeClass("button-selected").addClass("button-unselected");
        $(this).removeClass("button-unselected").addClass("button-selected");
        event.preventDefault();
        })

    $(WMStats.Globals.Event).on(E.LOADING_DIV_START, 
        function(event, data) {
            // TODO: need to update when partial_request happens)
            if (WMStats.Env.View === '#category_view' || 
                (WMStats.Env.View === '#request_view' && 
                 WMStats.Env.RequestSelection === "all_requests")) {
                     $('#loading_page').show();
                }
        });

    $(WMStats.Globals.Event).on(E.LOADING_DIV_END, 
        function(event, data) {
            $('#loading_page').hide();
        })
        
    $(WMStats.Globals.Event).on(E.AJAX_LOADING_START, 
        function(event, data) {
            $('#loading_page').show();
        });
})(jQuery);
