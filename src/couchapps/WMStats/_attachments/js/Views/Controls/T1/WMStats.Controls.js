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
            <input type="radio" name="category-select" value="requests" id="request-category" checked="checked"></input>\
            <label for="request-category">All Requests</label>\
            <input type="radio" name="category-select" value="campaign" id="campaign-category"></input>\
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
    
    
    return {
        setFilter: setFilter,
        setCategoryButton: setCategoryButton,
        getCategoryButtonValue: getCategoryButtonValue,
        getFilter: getFilter,
        requests: "requests",
        sites: "sites",
        campaign: "campaign"
    }
}(jQuery);
