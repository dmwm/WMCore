WMStats.namespace("Controls");
WMStats.Controls = function($){
    var _filterSelector;
    var _categorySelector;
    
    function setFilter(selector) {
        var inputFilter = '<div name="filter">\
                           workflow: <input name="workflow" value=""></input>\
                           </div>';
        $(selector).append(inputFilter);
        _filterSelector = selector + ' div[name="filter"] input';
    }
    
    function setCategoryButton(selector) {
        var categoryBottons = 
        '<nav class="category-button">\
            <input type="radio" name="category-select" value="requests" id="request-category">\
            <label for="request-category">All Requests</label>\
            <input type="radio" name="category-select" value="run" id="run-category"  checked="checked">\
            <label for="run-category">Run</label>\
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
        run: "run"
    }
}(jQuery);
