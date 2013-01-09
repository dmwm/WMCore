/*
 * Add EventHandler 
 */
(function($) {
    
    //custom events
    var E = WMStats.CustomEvents;
    
    var COL_INDEX = {};
    
    // Super class for table event handling
    function TableEventHandler(containerID, populateRequestTable) {
        this.containerID = containerID;
        if (populateRequestTable) {
            this.populateRequestTable = populateRequestTable;
        }
    }
    // add Classmethod and property
    TableEventHandler.highlightRow = function(selector, currenElement) {
                                        $(selector).removeClass('mouseclickRow');
                                        $(currenElement).addClass('mouseclickRow');
                                    }

    TableEventHandler.prototype = { 
        constructor: TableEventHandler,
        
        tableRowBind: function(bind, parentSelector, func) {
            var currentObj = this;
            var selector =  parentSelector + " table tbody tr"
            $(selector).live(bind, function () {
                TableEventHandler.highlightRow(selector, this);
                currentObj[func](this);
            });
        },

        tableColumnBind: function(bind, parentSelector, name, func) {
            var currentObj = this;
            var selector =  parentSelector + ' table tbody tr td div[name="' + name + '"]';
            var rowSelector = parentSelector + ' table tbody tr';
            $(document).on(bind, selector, function () {
                var currentRow = $(this).closest("tr");
                TableEventHandler.highlightRow(rowSelector, currentRow);
                currentObj[func](currentRow);
                event.preventDefault();
            });
        },
        
        populateRequestSummary: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var categoryKey = $(nTds[2]).text();
            
            // need to get from the cache
            var category = WMStats.Controls.getCategoryButtonValue();
            var categoryData = getCategorizedData(category);
            var requestData = categoryData.getRequestData(categoryKey);
            WMStats.Env.RequestSelection = "partial_requests";
            $(WMStats.Globals.Event).triggerHandler(E.REQUEST_SUMMARY_READY, requestData);
            $("#all_requests li a").removeClass("nav-button-selected").addClass("button-unselected");
        },
        
        populateJobSummary: function(currentElement){
            var nTds = $('td', currentElement);
            var requestName = $(nTds[2]).text();
            WMStats.JobSummaryModel.setRequest(requestName);
            $(WMStats.Globals.Event).triggerHandler(E.AJAX_LOADING_START);
            WMStats.JobSummaryModel.retrieveData();
        },
        
        populateRequestDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var workflowName = $(nTds[2]).text();
            $(WMStats.Globals.Event).triggerHandler(E.REQUEST_DETAIL_READY, workflowName);
        },

        populateCategoryDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var categoryKey = $(nTds[2]).text();
             // clean up job summary and detail view.
            $(WMStats.Globals.Event).triggerHandler(E.CATEGORY_DETAIL_READY, categoryKey);
        },

        populateJobDetail: function (currentElement){
            // 2. create the job detail view
            var nTds = $('td', currentElement);
            var summary = {};
            // need to get the workflow name with out depeniding on the selector
            summary.workflow = $("#job_view div.summary_data").data("workflow");
            summary.task = $(nTds[0]).text();
            summary.status = $(nTds[1]).text();
            summary.site = $(nTds[2]).text();
            if (summary.site ==="{}"){
                summary.site = {};
            }
            summary.exitCode = Number($(nTds[3]).text());
            WMStats.JobDetailModel.setOptions(summary);
            $(WMStats.Globals.Event).triggerHandler(E.AJAX_LOADING_START);
            WMStats.JobDetailModel.retrieveData();
        },
        
    };

    var ActiveModelHandler = new TableEventHandler();
        //ActiveModelHandler.tableRowBind("click", "#category_view div.summary_data",
        //                                 "populateRequestSummary");
        ActiveModelHandler.tableColumnBind('click', "#category_view div.summary_data", "drill",
                                        "populateRequestSummary");
        ActiveModelHandler.tableColumnBind("click", "#request_view div.summary_data", "drill",
                                        "populateJobSummary");
        ActiveModelHandler.tableRowBind("click","#job_view div.summary_data", 
                                         "populateJobDetail");
        //ActiveModelHandler.tableRowBind('mouseover', "#category_view div.summary_data", 
        //                                "populateCategoryDetail");
        ActiveModelHandler.tableColumnBind('click', "#category_view div.summary_data", "detail",
                                        "populateCategoryDetail");
        ActiveModelHandler.tableColumnBind('click', "#request_view div.summary_data", "detail",
                                        "populateRequestDetail");

    // actual event binding codes
    // row mouse over/ mouse out events
    $('tr').live('mouseover', function(event) {
        $(this).addClass('mouseoverRow')
    });
    
    $('tr').live('mouseout', function(event) {
        $(this).removeClass('mouseoverRow')
    });

})(jQuery);
