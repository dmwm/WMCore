/*
 * Add EventHandler 
 */
(function($) {
    
    //custom events
    var E = WMStats.CustomEvents;

    // Super class for table event handling
    function TableEventHandler(containerID, populateRequestTable) {
        this.containerID = containerID;
        if (populateRequestTable) {
            this.populateRequestTable = populateRequestTable;
        }
    }
    // add Classmethod and property
    TableEventHandler.highlightRow = function(selector, currenElement) {
                                        $(selector).removeClass('green');
                                        $(currenElement).addClass('green');
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

        populateRequestSummary: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var categoryKey = $(nTds[0]).text();
            
            // need to get from the cache
            var category = WMStats.Controls.getCategoryButtonValue();
            var categoryData = getCategorizedData(category);
            var requestData = WMStats.Requests();
            requestData.setDataByWorkflow(categoryData.getData(categoryKey).requests);
            
            $(WMStats.Globals.Event).triggerHandler(E.REQUEST_SUMMARY_READY, requestData);
        },
        
        populateJobSummary: function(currentElement){
            var nTds = $('td', currentElement);
            var requestName = $(nTds[0]).text();
            WMStats.JobSummaryModel.setRequest(requestName);
            WMStats.JobSummaryModel.retrieveData();
        },
        
        populateRequestDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var workflowName = $(nTds[0]).text();
            $(WMStats.Globals.Event).triggerHandler(E.REQUEST_DETAIL_READY, workflowName);
        },

        populateCategoryDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var categoryKey = $(nTds[0]).text();
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
            WMStats.JobDetailModel.retrieveData();
        },
        
    };
    
    // create new instance and activate the event hander
    /*
    var OverviewHandler = new TableEventHandler("#data_board");
        OverviewHandler.addTableEvents();
    
    var SiteModelHandler = new TableEventHandler("#tab-site", 
        function(currentElement) {
            var row = $('td', currentElement);
            var site = $(row[0]).text();
            var agentUrl = $(row[1]).text();
            var objPtr = this;
            var requestList = function(data) {
                var keys = [];
                for (var i in data.rows){
                    keys.push(data.rows[i].key[2]);
                    }
                 return keys;
            }
                
            function getRequests(data) {
                var options = {'keys': requestList(data), 'include_docs': true}
                objPtr.overviewAction("allDocs", options);
            }
            
            var options = {'startkey':[site, agentUrl], 
                           'endkey':[site, agentUrl, {}],
                           'reduce': true, "group_level": 3};
            var viewName = 'latestAgentSite';
            WMStats.Couch.view(viewName, options, getRequests);
            
       })
       SiteModelHandler.addTableEvents();
       
    var AlertModelHandler = new TableEventHandler("#tab-alert", null);
        AlertModelHandler.addTableEvents();
    */
   
    var ActiveModelHandler = new TableEventHandler();
        ActiveModelHandler.tableRowBind("click", "#category_view div.summary_data",
                                         "populateRequestSummary");
        ActiveModelHandler.tableRowBind("click", "#request_view div.summary_data", 
                                        "populateJobSummary");
        ActiveModelHandler.tableRowBind("click","#job_view div.summary_data", 
                                         "populateJobDetail");
        ActiveModelHandler.tableRowBind('mouseover', "#category_view div.summary_data", 
                                        "populateCategoryDetail");
        ActiveModelHandler.tableRowBind('mouseover', "#request_view div.summary_data",
                                        "populateRequestDetail");
                                        
        //ActiveModelHandler.tableRowClick(TableEventHandler.reqSummaryDiv , "populateRequestDetail", 'mouseover');
    // actual event binding codes
    // row mouse over/ mouse out events
    $('tr').live('mouseover', function(event) {
        $(this).addClass('yellow')
    });
    
    $('tr').live('mouseout', function(event) {
        $(this).removeClass('yellow')
    });

})(jQuery)
