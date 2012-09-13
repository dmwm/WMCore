/*
 * Add EventHandler 
 */
(function($) {
    
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
    TableEventHandler.reqSummaryDiv = "requestSummary";
    TableEventHandler.reqDetailDiv = "requestDetail";
    TableEventHandler.jobSummaryDiv = "jobSummary";
    TableEventHandler.jobDetailDiv = "jobDetail";
    
    TableEventHandler.prototype = { 
        constructor: TableEventHandler,
        
        tableRowClick: function(divName, funcName, bind) {
            var bind = bind ||'click' 
            var currentObj = this;
            var selector = this.containerID + " > div[name='" + divName + "'] table tbody tr"
            $(selector).live(bind, function () {
                TableEventHandler.highlightRow(selector, this);
                currentObj[funcName](this);
            });
        },
        
        containerSelector: function(divName) {
            return this.containerID +  " > div[name='" + divName + "'] > div.body";
        },
        
        overviewAction: function(viewName, options) {
            $(this.containerSelector(TableEventHandler.jobSummaryDiv)).empty();
            $(this.containerSelector(TableEventHandler.jobDetailDiv)).empty();
            
            WMStats.RequestModel.retrieveData(viewName, options);
        },
    
        populateRequestTable: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var campaignName = $(nTds[0]).text();
            var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 
                           'include_docs': true};
            var viewName = 'requestByCampaignAndDate';
    
             // clean up job summary and detail view. 
            this.overviewAction(viewName, options);
        },
        
        populateRequestDetail: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var workflowName = $(nTds[0]).text();
             // clean up job summary and detail view.
            WMStats.RequestDetailList(workflowName,
                      this.containerID +  " div[name='" + TableEventHandler.reqDetailDiv + "']")
        },
        
        populateRequestTable: function(currentElement){
            // create the request table
            var nTds = $('td', currentElement);
            var campaignName = $(nTds[0]).text();
            var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 
                           'include_docs': true};
            var viewName = 'requestByCampaignAndDate';
    
             // clean up job summary and detail view. 
            this.overviewAction(viewName, options);
        },
        
        populateJobTable: function(currentElement){
            var nTds = $('td', currentElement);
            var requestName = $(nTds[0]).text();
            WMStats.JobSummaryModel.setRequest(requestName);
            WMStats.JobSummaryModel.draw(this.containerSelector(TableEventHandler.jobSummaryDiv));
            
            // 3. clean up job detail view.
            $(this.containerSelector(TableEventHandler.jobDetailDiv)).empty();
        },
        
        populateJobDetail: function (currentElement){
            // 2. create the job detail view
            var nTds = $('td', currentElement);
            var summary = {};
            
            summary.workflow = $(this.containerSelector(TableEventHandler.jobSummaryDiv)).data("workflow");
            summary.status = $(nTds[0]).text();
            summary.site = $(nTds[1]).text();
            if (summary.site ==="{}"){
                summary.site = {};
            }
            summary.exitCode = Number($(nTds[2]).text());
            WMStats.JobDetailModel.setOptions(summary);
            WMStats.JobDetailModel.draw(this.containerSelector(TableEventHandler.jobDetailDiv));
        },
        
        addTableEvents: function() {
            this.tableRowClick('overview', "populateRequestTable");
            this.tableRowClick('requestSummary', "populateJobTable");
            this.tableRowClick('jobSummary', "populateJobDetail");
        }
    };
    
    // create new instance and activate the event hander
    var OverviewHandler = new TableEventHandler("#tab-overview");
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
        
    var ActiveModelHandler = new TableEventHandler("#tab-active-request", null);
        ActiveModelHandler.tableRowClick('requestSummary', "populateRequestDetail", 'mouseover');
        ActiveModelHandler.addTableEvents();
    // actual event binding codes
    // row mouse over/ mouse out events
    $('tr').live('mouseover', function(event) {
        $(this).addClass('yellow')
    });
    
    $('tr').live('mouseout', function(event) {
        $(this).removeClass('yellow')
    });

})(jQuery)
