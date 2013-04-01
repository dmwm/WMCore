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
            var categoryKey = $(nTds[2]).text();
            // need to get from the cache
            var category = WMStats.Controls.getCategoryButtonValue();
            var categoryData = getCategorizedData(category);
            var requestData = categoryData.getRequestData(categoryKey);
            WMStats.CategoryTitle(categoryKey, '#category_title');
            WMStats.Env.RequestSelection = "partial_requests";
            $(WMStats.Globals.Event).triggerHandler(E.REQUEST_SUMMARY_READY, requestData);
            $("#all_requests li a").removeClass("nav-button-selected").addClass("button-unselected");
        },
        
        populateJobSummary: function(currentElement){
            var nTds = $('td', currentElement);
            var requestName = $(nTds[2]).text();
            WMStats.RequestTitle(requestName, '#request_title');
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
            var summary = {};
            // need to get the workflow name with out depeniding on the selector
            summary.workflow = $("#job_view div.summary_data").data("workflow");
            //summary.task = $(nTds[0]).text();
            var currentRow = $(currentElement).closest("tr")[0]
            var currentRowData = WMStats.Env.JobSummaryTable.fnGetData(currentElement);
            summary.task = currentRowData.task;
            summary.status = currentRowData.status;
            summary.site = currentRowData.site;
            if (summary.site ==="{}"){
                summary.site = {};
            }
            summary.acdcURL = currentRowData.acdcURL
            summary.exitCode = currentRowData.exitCode;
            WMStats.JobDetailModel.setOptions(summary);
            $(WMStats.Globals.Event).triggerHandler(E.AJAX_LOADING_START);
            WMStats.JobDetailModel.retrieveData();
        },
        
        createACDCResubmission: function (currentElement){
            var workflow = $("#job_view div.summary_data").data("workflow");
            var summary = WMStats.ViewModel.Resubmission;
            summary.OriginalRequestName = workflow;
            //TODO: should be getting most information from request mgr database
            var requestData = WMStats.ActiveRequestModel.getData().getDataByWorkflow(summary.OriginalRequestName);
            summary.RequestString = WMStats.Utils.acdcRequestSting(summary.OriginalRequestName, requestData.requestor)
            var currentRow = $(currentElement).closest("tr")[0]
            var currentRowData = WMStats.Env.JobSummaryTable.fnGetData(currentElement);
            summary.InitialTaskPath = currentRowData.task;
            if (currentRowData.acdcURL) {
                //if there is not acdc_url don't create the button'
                var acdcServiceUrl = WMStats.Utils.splitCouchServiceURL(currentRowData.acdcURL);
                summary.ACDCServer = acdcServiceUrl.couchUrl;
                summary.ACDCDatabase = acdcServiceUrl.couchdb;
            }
            summary.Group = requestData.group;
            summary.PrepID = requestData.prep_id;
            summary.RequestPriority = requestData.priority;
            summary.DbsUrl = requestData.dbs_url || "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet";
            summary.Memory = requestData.Memory || 2394967;
            summary.SizePerEvent = requestData.SizePerEvent || 5000;
            summary.TimePerEvent = requestData.TimePerEvent || 60;
            summary.RequestType = "Resubmission";
            
            WMStats.ReqMgrRequestModel.retrieveDoc(workflow);
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
        $(this).addClass('mouseoverRow')
    });
    
    $(document).on('mouseout', 'tr', function(event) { 
        $(this).removeClass('mouseoverRow')
    });

})(jQuery);
