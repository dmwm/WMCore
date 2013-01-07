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
                        <select name="SearchOptions" class="searchSelector" title="Select a search option">\
                          <option value="request" data-search-type="stringMatch" selected="selected"> request name</option>\
                          <option value="outputdataset" data-search-type="stringMatch"> output dataset </option>\
                          <option value="inputdataset" data-search-type="stringMatch">input dataset</option>\
                         </select>\
                         <input type="text" size="100" name="workloadSummarySearch" value=""></input>\
                         <button type="submit" id="WorkloadSummarySearchButton">submit</button>\
                     </div></fieldset>';
        $(selector).append(searchOption);
    };
    
    return {
        setUTCClock: setUTCClock,
        setLinkTabs: setLinkTabs,
        setWorkloadSummarySearch: setWorkloadSummarySearch
    }
}(jQuery);