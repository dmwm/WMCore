WMStats.namespace("Controls");
WMStats.Controls = function($){
    
    var _filterSelector;
    var _categorySelector;
    
    function setFilter(selector) {
       $(selector).append('<div name="filter">\
                            campaign: <input name="campaign" value=""></input>\
                            workflow: <input name="workflow" value=""></input>\
                            type: <input name="request_type" value=""></input>\
                            status: <input name="request_status" value=""></input>\
                            input dataset: <input name="inputdataset" value=""></input>\
                            output dataset: <input name="outputdatasets" value=""></input>\
                            site whitelist: <input name="site_white_list" value=""></input>\
                            agent: <input name="agent_url" value=""></input>\
                           </div>');
       _filterSelector = selector + ' div[name="filter"] input';
    };

    function setCategoryButton(selector) {
        var categoryBottons = 
        '<nav id="category_button" class="button-group">\
            <ul><li><a href="#campaign" class="nav-button nav-button-selected"> Campaign </a></li>\
                <li><a href="#sites" class="nav-button button-unselected"> Site </a></li></ul>\
         </nav>';
        
        $(selector).append(categoryBottons);
        WMStats.Env.CategorySelection = "campaign";
    };
    
    function setViewSwitchButton(selector) {
        var viewSwitchBottons = 
        '<nav id="view_switch_button" class="button-group">\
            <ul><li><a href="#progress" class="nav-button nav-button-selected"> progress </a></li>\
                <li><a href="#numJobs" class="nav-button button-unselected"> number of jobs </a></li></ul>\
         </nav>';
        
        $(selector).append(viewSwitchBottons);
        WMStats.Env.ViewSwitchSelection = "progress";
    };
    
    function setAllRequestButton(selector) {
        var requestBottons = 
        '<nav id="all_requests" class="button-group">\
            <ul><li><a href="#" class="nav-button"> all requests </a></li></ul>\
        </nav>';
        
        $(selector).append(requestBottons).addClass("button-group");
        WMStats.Env.RequestSelection = "all_requests";
    };
    
    function getCategoryButtonValue() {
         return WMStats.Env.CategorySelection;
    };
    
    function getFilter() {
        return WMStats.Utils.createInputFilter(_filterSelector);
    };
    
    function setTabs(selector) {
        var tabs = '<ul><li class="first"><a href="#category_view">Category</a></li>\
                    <li><a href="#request_view">&#187 Requests</a></li>\
                    <li><a href="#job_view">&#187 Jobs</a></li></ul>'
        $(selector).append(tabs).addClass("tabs");
        $(selector + " ul").addClass("tabs-nav");
    };
    
    return {
        setFilter: setFilter,
        setTabs: setTabs,
        setCategoryButton: setCategoryButton,
        setAllRequestButton: setAllRequestButton,
        getCategoryButtonValue: getCategoryButtonValue,
        setViewSwitchButton: setViewSwitchButton,
        getFilter: getFilter,
        requests: "requests",
        sites: "sites",
        campaign: "campaign"
    }
}(jQuery);
