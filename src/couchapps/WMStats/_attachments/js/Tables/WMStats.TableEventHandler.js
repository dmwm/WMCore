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
            
            WMStats.RequestView.draw(this.containerSelector(TableEventHandler.reqSummaryDiv), 
                                            viewName, options)
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
            WMStats.RequestDetailView.createDetailView(this.containerID +  " > div[name='" + TableEventHandler.reqDetailDiv + "']",
                                                   workflowName)
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
            WMStats.JobSummaryView.createSummaryView(this.containerSelector(TableEventHandler.jobSummaryDiv),
                                                     requestName);
            
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
            summary.exitCode = Number($(nTds[2]).text());
            WMStats.JobDetailView.createDetailView(this.containerSelector(TableEventHandler.jobDetailDiv), 
                                                   summary);
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
    
    var SiteViewHandler = new TableEventHandler("#tab-site", 
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
       SiteViewHandler.addTableEvents();
       
    var AlertViewHandler = new TableEventHandler("#tab-alert", null);
        AlertViewHandler.addTableEvents();
        
    var ActiveViewHandler = new TableEventHandler("#tab-active-request", null);
        ActiveViewHandler.tableRowClick('requestSummary', "populateRequestDetail", 'mouseover');
        ActiveViewHandler.addTableEvents();
    // actual event binding codes
    // row mouse over/ mouse out events
    $('tr').live('mouseover', function(event) {
        $(this).addClass('yellow')
    });
    
    $('tr').live('mouseout', function(event) {
        $(this).removeClass('yellow')
    });
    
    // collapsible bar
    $('div.caption img').live('click', function(event){
        $(this).parent('div.caption').siblings('div.body').toggle('nomal');
    });

    
    $('div').ajaxSend(function(){
            alert("test");
            $(this).show();
        }).live("ajaxComplete", function(){
            $(this).hide();
    });

})(jQuery)
