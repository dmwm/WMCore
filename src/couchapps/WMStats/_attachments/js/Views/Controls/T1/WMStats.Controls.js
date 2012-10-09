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
                           </div>');
       _filterSelector = selector + ' div[name="filter"] input';
    };
    
    function setCategoryButton(selector) {
        var categoryBottons = 
        '<nav class="category-button">\
            <input type="radio" name="category-select" value="requests" id="request-category"></input>\
            <label for="request-category">All Requests</label>\
            <input type="radio" name="category-select" value="campaign" id="campaign-category" checked="checked"></input>\
            <label for="campaign-category">Campaign</label>\
            <input type="radio" name="category-select" value="sites" id="site-category"></input>\
            <label for="site-category">Site</label>\
         </nav>';
        
        $(selector).append(categoryBottons);
        _categorySelector = selector + ' input[name="category-select"][type="radio"]:checked';
        
    };
    
    function getCategoryButtonValue() {
         return $(_categorySelector).val();
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
        getCategoryButtonValue: getCategoryButtonValue,
        getFilter: getFilter,
        requests: "requests",
        sites: "sites",
        campaign: "campaign"
    }
}(jQuery);
