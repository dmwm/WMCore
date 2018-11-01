WMStats.namespace("Controls");
WMStats.Controls = function($){
    
    var vm =  WMStats.ViewModel;
    var vmRegistry = WMStats.ViewModel.Registry;
    
    var _categorySelector;
    
    function setFilter(selector) {
       $(selector).append('<legend>filter</legend><div name="filter" class="filterFormat">\
                            <div class="verticalFilter"> campaign: <br/><input name="campaign" value=""></input> </div> \
                            <div class="verticalFilter"> workflow: <br/><input name="workflow" value=""></input> </div>\
                            <div class="verticalFilter"> type: <br/><input name="request_type" value=""></input> </div>\
                            <div class="verticalFilter"> status: <br/><input name="request_status" value=""></input> </div>\
                            <div class="verticalFilter"> input dataset: <br/><input name="inputdataset" value=""></input> </div>\
                            <div class="verticalFilter"> output dataset: <br/><input name="outputdatasets" value=""></input> </div>\
                            <div class="verticalFilter"> site whitelist: <br/><input name="site_white_list" value=""></input> </div>\
                            <div class="verticalFilter"> team: <br/><input name="team" value=""></input> </div> \
                            <div class="endFlter"> agent:<br/><input name="agent_url" value=""></input> </div>\
                           </div>');
       var _filterSelector = selector + ' div[name="filter"] input';
       
       $(document).on('keyup', selector + " input", 
                function() {WMStats.Utils.delay(function() {
                    //change the view model filter value
                    WMStats.ViewModel.ActiveRequestPage.filter(WMStats.Utils.createInputFilter(_filterSelector));
                    
                }, 300)});
    };

    function setCategoryButton(selector) {
        var categoryBottons = 
        '<nav id="category_button" class="button-group">\
            <ul><li><a href="#campaign" class="nav-button nav-button-selected"> Campaign </a></li>\
                <li><a href="#sites" class="nav-button button-unselected"> Site </a></li>\
                <li><a href="#cmssw" class="nav-button button-unselected"> CMSSW </a></li>\
                <li><a href="#agent" class="nav-button button-unselected"> Agent </a></li></ul>\
         </nav>';
        
        $(selector).append(categoryBottons);
        
        $(document).on('click', selector + " li a", function(event){
            var category = this.hash.substring(1);
            var vmCategory;
            if (category === "campaign") {
                vmCategory = vm.CampaignCategory;
            } else if (category === "sites") {
                vmCategory = vm.SiteCategory;
            } else if (category === "cmssw") {
                vmCategory = vm.CMSSWCategory;
            } else if (category === "agent") {
                vmCategory = vm.AgentCategory;
            }
            vm.CategoryView.category(vmCategory);
            $(selector + " li a").removeClass("nav-button-selected").addClass("button-unselected");
            $(this).addClass("nav-button-selected");
            event.preventDefault();
        });
    };

    function setViewSwitchButton(selector) {
        var viewSwitchBottons = 
        '<nav id="view_switch_button" class="button-group">\
            <ul><li><a href="#progress" class="nav-button nav-button-selected"> progress </a></li>\
                <li><a href="#numJobs" class="nav-button button-unselected"> number of jobs </a></li></ul>\
         </nav>';
        
        $(selector).append(viewSwitchBottons);

        $(document).on('click', "#view_switch_button li a", function(event){
            // format is either progress or numJobs need to decouple the name
            var buttonName = this.hash.substring(1);
            var requestFormat;
            if (buttonName === "progress") {
                requestFormat = vm.RequestProgress;
            } else if (buttonName === "numJobs") {
                requestFormat = vm.RequestJobs;
            }
            
            vm.RequestView.format(requestFormat);
            // this might not be the efficient way. or directly update the table.
            vm.RequestView.propagateUpdate();
            $("#view_switch_button li a").removeClass("nav-button-selected").addClass("button-unselected");
            $(this).addClass("nav-button-selected");
            event.preventDefault();
        });
        
    };
    
    function setAllRequestButton(selector) {
        var requestButtons = 
        '<nav id="all_requests" class="button-group">\
            <ul><li><a href="#" class="nav-button"> all requests </a></li></ul>\
        </nav>';
        
        $(selector).append(requestButtons).addClass("button-group");
        
        $(document).on('click', "#all_requests li a", function(event){
            vm.RequestView.categoryKey("all");
            event.preventDefault();
           });
        
        vm.RequestView.subscribe("categoryKey", function(){
            var buttonSelector = "#all_requests li a";
            if (vm.RequestView.categoryKey() === "all") {
                $(buttonSelector).removeClass("button-unselected").addClass("nav-button-selected");
            } else {
                $(buttonSelector).removeClass("nav-button-selected").addClass("button-unselected");
            }
        });    
    };

    /* set the view tab and control*/
    function setTabs(selector) {
        var tabs = '<ul><li class="first"><a href="#category_view">Category</a></li>\
                    <li><a href="#request_view">&#187 Requests</a></li>\
                    <li><a href="#job_view">&#187 Jobs</a></li></ul>';
        $(selector).append(tabs).addClass("tabs");
        $(selector + " ul").addClass("tabs-nav");
        
        // add controller for this view
        function changeTab(event, data) {
            $(selector + ' li').removeClass("tabs-selected");
            $(selector + ' a[href="' + data.id() +'"]').parent().addClass("tabs-selected");
        }
        // viewModel -> view control
        vm.ActiveRequestPage.subscribe("view", changeTab);
        
        // view -> viewModel control
        $(document).on('click', selector + " li a", function(event){
            vm.ActiveRequestPage.view(vmRegistry[this.hash]);
            event.preventDefault();
        });

    };
    

    function setExternalLink(selector) {
        var outsideLink = 
        '<a href="/couchdb/alertscollector/_design/AlertsCollector/index.html" target="alertColletorFrame"> agent alert </a>';
        
        $(selector).append(outsideLink);
    };
    

    return {
        setFilter: setFilter,
        setTabs: setTabs,
        setCategoryButton: setCategoryButton,
        setAllRequestButton: setAllRequestButton,
        setViewSwitchButton: setViewSwitchButton,
        setExternalLink: setExternalLink,
        requests: "requests",
        sites: "sites",
        campaign: "campaign",
        cmssw: "cmssw"
    };
}(jQuery);
