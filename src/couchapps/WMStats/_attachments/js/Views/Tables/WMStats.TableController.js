/*
 * Add EventHandler 
 */
(function($) {
    
    //custom events
    var E = WMStats.CustomEvents;
    var vm =  WMStats.ViewModel;

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
                                   };

    TableEventHandler.prototype = { 
        constructor: TableEventHandler,
        
        tableRowBind: function(bind, parentSelector, func) {
            var currentObj = this;
            var selector =  parentSelector + " table tbody tr";
            $(document).on(bind, selector, function () {
                TableEventHandler.highlightRow(selector, this);
                currentObj[func](this);
            });
        },

        tableColumnBind: function(bind, parentSelector, name, func) {
            var currentObj = this;
            var selector =  parentSelector + ' table tbody tr td div[name="' + name + '"]';
            var rowSelector = parentSelector + ' table tbody tr';
            $(document).on(bind, selector, function () {
                var currentRow = $(this).closest("tr")[0];
                TableEventHandler.highlightRow(rowSelector, currentRow);
                currentObj[func](currentRow);
                event.preventDefault();
            });
        },
        
        populateRequestSummary: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            vm.RequestView.categoryKey($(nTds[2]).text());
        },
        
        populateJobSummary: function(currentElement){
            
            var nTds = $('td', currentElement);
            vm.JobView.requestName($(nTds[2]).text());
        },
        
        populateRequestDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            vm.RequestDetail.requestName($(nTds[2]).text());
        },

        populateCategoryDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            vm.CategoryDetail.categoryKey($(nTds[2]).text());
        },

        populateJobDetail: function (currentElement){
            // 2. create the job detail view
            var summary = {};
            // need to get the workflow name with out depending on the selector
            summary.workflow = $("#job_view div.summary_data").data("workflow");
            //summary.task = $(nTds[0]).text();
            var currentRow = $(currentElement).closest("tr")[0];
            var currentRowData = WMStats.JobSummaryTable.data.row(currentElement).data();
            summary.task = currentRowData.task;
            summary.status = currentRowData.status;
            summary.site = currentRowData.site;
            if (summary.site ==="{}"){
                summary.site = {};
            }
            summary.acdcURL = currentRowData.acdcURL;
            summary.exitCode = currentRowData.exitCode;
            
            vm.JobDetail.keys(summary);
            $(WMStats.Globals.Event).triggerHandler(E.AJAX_LOADING_START);
        },
        
        createACDCResubmission: function (currentElement){
            var workflow = $("#job_view div.summary_data").data("workflow");
            var summary = {};
            summary.requestName = workflow;
            //TODO: should be getting most information from request mgr database
            var currentRowData = WMStats.JobSummaryTable.data.row(currentElement).data();
            summary.task = currentRowData.task;
            summary.acdcURL = currentRowData.acdcURL;
            vm.Resubmission.keys(summary);
        }
    };

    var ActiveModelHandler = new TableEventHandler();
        ActiveModelHandler.tableColumnBind('click', "#category_view div.summary_data", "drill",
                                        "populateRequestSummary");
        ActiveModelHandler.tableColumnBind("click", "#request_view div.summary_data", "drill",
                                        "populateJobSummary");
        //ActiveModelHandler.tableRowBind("click","#job_view div.summary_data", 
        //                                "populateJobDetail");
        ActiveModelHandler.tableColumnBind("click","#job_view div.summary_data", "drill",
                                         "populateJobDetail");
        ActiveModelHandler.tableColumnBind("click","#job_view div.summary_data", "acdc",
                                         "createACDCResubmission");
        ActiveModelHandler.tableColumnBind('click', "#category_view div.summary_data", "detail",
                                        "populateCategoryDetail");
        ActiveModelHandler.tableColumnBind('click', "#request_view div.summary_data", "detail",
                                        "populateRequestDetail");

    // actual event binding codes
    // row mouse over/ mouse out events
    $(document).on('mouseover', 'tr', function(event) {
        $(this).addClass('mouseoverRow');
    });
    
    $(document).on('mouseout', 'tr', function(event) { 
        $(this).removeClass('mouseoverRow');
    });

})(jQuery);
