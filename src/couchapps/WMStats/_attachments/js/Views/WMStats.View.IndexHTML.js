WMStats.namespace("View");

WMStats.View.IndexHTML = function(){
    
    function applyTemplate(){
        var viewPane = $('#data_board div.viewPane');
        $('div.viewTemplate').children().clone().appendTo(viewPane);
    };
    
    function retrieveData() {
        WMStats.ActiveRequestModel.retrieveData();
        WMStats.AgentModel.retrieveData();
        //WMStats.HistoryModel.setOptions();
        //WMStats.HistoryModel.retrieveData();
    };

    $(document).ready(function() {
        $('#loading_page').addClass("front").show();
        //applyTemplate();
        WMStats.CommonControls.setLinkTabs("#link_tabs");
        //WMStats.Controls.setExternalLink("#external_link");
        WMStats.Controls.setDBSourcetButton("#external_link");
        WMStats.CommonControls.setUTCClock("#clock");
        WMStats.CommonControls.setWorkloadSummarySearch("#search_option_board");
        WMStats.Controls.setFilter("#filter_board");
        WMStats.Controls.setAllRequestButton("#status_board");
        WMStats.Controls.setTabs("#tab_board");
        WMStats.Controls.setCategoryButton("#category_bar");
        WMStats.Controls.setViewSwitchButton("#view_switch_bar");
        
        //view model bind
        var vm = WMStats.ViewModel;
        vm.ActiveRequestPage.id('#activeRequestPage');
        vm.AgentPage.id('#agentInfoPage');
        vm.RequestAlertPage.id('#requestAlertPage');
        vm.SearchPage.id('#workloadSummaryPage');
        
        vm.CategoryView.id('#category_view');
        vm.RequestView.id('#request_view');
        vm.JobView.id('#job_view');
        
        // Category summary view
        vm.CampaignCategory.id('#category_view div.summary_data');
        vm.SiteCategory.id('#category_view div.summary_data');
        vm.CMSSWCategory.id('#category_view div.summary_data');
        vm.AgentCategory.id('#category_view div.summary_data');
        
        // Request summary view
        vm.RequestProgress.id('#request_view div.summary_data');
        vm.RequestJobs.id('#request_view div.summary_data');
        
        // Job summary view
        //To do  need to add
        
        vm.CategoryDetail.id('#category_view div.detail_data');
        vm.RequestDetail.id('#request_view div.detail_data');
        vm.JobDetail.id('#job_view div.detail_data');
        vm.Resubmission.id('#acdc_submission');
        
       
        // request alert view
        vm.AlertJobView.id("#alert_job_summary");
        vm.AlertJobDetail.id("#alert_job_detail");
        
        // view model controller
        var wsControl = WMStats.GenericController;
        
        //callback funtion
        function switchPage(event, data) {
            //data is page object
            wsControl.switchDiv(data.id(), ["#activeRequestPage", "#requestAlertPage", 
                                            "#agentInfoPage", "#workloadSummaryPage"]);
            vm.propagateUpdate();
        };
        
        function switchView(event, data) {
            //data is page object
            wsControl.switchDiv(data.id(), ["#category_view", "#request_view", "#job_view"]);
            vm.ActiveRequestPage.view().propagateUpdate();
        };
        
        // set the control bind
        vm.subscribe("page", switchPage);
        vm.ActiveRequestPage.subscribe("view", switchView);
        vm.RequestView.subscribe("categoryKey", function(event, categoryKey) {
             WMStats.CategoryTitle(categoryKey, '#category_title');
        });
        
        vm.JobView.subscribe("requestName", function(event, requestName) {
             WMStats.RequestTitle(requestName, '#request_title');
        });
        
        var E = WMStats.CustomEvents;
        $(WMStats.Globals.Event).on(E.AGENTS_LOADED, function(event, agentData) {
        	// refresh the agentData.agentNumber.error
        	agentData.getAlertList();
            if (agentData.agentNumber.error > 0) {
                $('#linkTabs a[href="#agentInfoPage"] strong').text("(" + agentData.agentNumber.error + ")");
            }
        });
        
        $(WMStats.Globals.Event).on(E.REQUESTS_LOADED, 
            function(event) {
                //only Tier1 case
                var numError = WMStats.ActiveRequestModel.getData().numOfRequestError();
                if ((numError.alert + numError.stalled) > 0) {
                    $('#linkTabs a[href="#requestAlertPage"] strong').text("(" + 
                                      numError.stalled + ", " + numError.alert  + ")");
                };
        });
        
        // initialize page)
        vm.page(vm.ActiveRequestPage);
        vm.ActiveRequestPage.view(vm.CategoryView);
        //vm.CategoryView.subscribe("ecategory", switchCategory) 
        retrieveData();
        //$("div.draggable").draggable();
        // 5 min update
        setInterval(retrieveData, 300000);
     } );
};
