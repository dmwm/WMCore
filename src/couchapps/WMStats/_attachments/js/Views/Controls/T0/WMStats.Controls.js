WMStats.namespace("Controls");
WMStats.Controls = function($){
    var _filterSelector;
    var _categorySelector;
    
    function setFilter(selector) {
        var inputFilter = '<div name="filter">\
                           workflow: <input name="workflow" value=""></input>\
                           status: <input name="request_status" value=""></input>\
                           run: <input name="run" value=""></input>\
                           </div>';
        $(selector).append(inputFilter);
        _filterSelector = selector + ' div[name="filter"] input';
    }
    
    function setCategoryButton(selector) {
        /*
        var categoryBottons = 
        '<nav id="category_button">\
            <ul><li class="nav-button button-selected"><a href="#run"> Run </a></li>\
         </nav>';
        
        $(selector).append(categoryBottons);
        */
        WMStats.Env.CategorySelection = "run";
    };
    
    function setAllRequestButton(selector) {
        var requestBottons = 
        '<nav id="all_requests" class="button_group">\
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
        var tabs = '<ul><li class="first"><a href="#category_view">Run</a></li>\
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
        getFilter: getFilter,
        requests: "requests",
        sites: "sites",
        run: "run"
    }
}(jQuery);
