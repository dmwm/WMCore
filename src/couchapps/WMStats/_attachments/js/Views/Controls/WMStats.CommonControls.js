WMStats.namespace("CommonControls");
WMStats.CommonControls = function($){
    
    function setUTCClock(selector) {
       setInterval(function() {
            $(selector).text(WMStats.Utils.utcClock());
        }, 100)
    };

    function setLinkTabs(selector) {
       var linkTabs = 
        '<nav id="linkTabs" class="linkTabs">\
            <ul><li><a href="#activeRequestPage"> active request </a></li>\
                <li><a href="#workloadSummaryPage"> search </a></li></ul>\
         </nav>';
        
        $(selector).append(linkTabs);
    };
    function setWorkloadSummarySearch(selector) {
        var searchOption =
                '<fieldset id="SearchOptionsPane">\
                    <legend>Search WorkloadSummary</legend>\
                    <div id="searchPane">\
                        <div class="OptionBox">\
                            <select name="SearchOptions" class="searchSelector" title="Select a search option">\
                              <option value="request" data-search-type="stringMatch" selected="selected"> request name</option>\
                              <option value="outputdataset" data-search-type="stringMatch"> output dataset </option>\
                              <option value="inputdataset" data-search-type="stringMatch">input dataset</option>\
                              <option value="prep_id" data-search-type="stringMatch">prep id</option>\
                              <option value="request_date" data-search-type="dateRange">date range</option>\
                            </select>\
                        </div>\
                        <div class="SearchBox">\
                           <input type="text" size="100" name="workloadSummarySearch" value=""></input>\
                        </div>\
                     </div>\
                     <div>\
                        <button type="submit" id="WorkloadSummarySearchButton">submit</button>\
                     </div>\
                     </fieldset>';
                     
        $(selector).append(searchOption);
    };
    
    $(document).on('change', 'select[name="SearchOptions"]',function(){
        var filterType = $(':selected', this).attr('data-search-type');
        var searchBox = $('#searchPane .SearchBox');
        $(searchBox).empty();
        $('div.template.'+ filterType).children().clone().appendTo('#searchPane .SearchBox');
        $('#searchPane .SearchBox input[name="dateRange1"]').datepicker({
            altField: 'input[name="dateRange1"]', 
            altFormat: "yy/mm/dd", 
            changeYear: true, 
            yearRange: "2012:c"});
        $('#searchPane .SearchBox input[name="dateRange2"]').datepicker({
            altField: 'input[name="dateRange2"]', 
            altFormat: "yy/mm/dd", 
            changeYear: true, 
            yearRange: "2012:c"});
    });
    
    return {
        setUTCClock: setUTCClock,
        setLinkTabs: setLinkTabs,
        setWorkloadSummarySearch: setWorkloadSummarySearch
    }
}(jQuery);