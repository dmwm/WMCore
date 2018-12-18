WMStats.namespace("CommonControls");
WMStats.CommonControls = function($){
    
    var vm =  WMStats.ViewModel;
    var vmRegistry = WMStats.ViewModel.Registry;
    
    function setUTCClock(selector) {
       setInterval(function() {
            $(selector).text(WMStats.Utils.utcClock());
        }, 100);
    };

    function setLinkTabs(selector) {
       var linkTabs = 
        '<nav id="linkTabs" class="linkTabs">\
            <ul><li><a href="#activeRequestPage"> active request </a></li>\
                <li><a href="#requestAlertPage"> request alert <strong></strong></a></li>\
                <li><a href="#agentInfoPage"> agent info <strong></strong></a></li>\
                <li><a href="#logDBPage"> error logs <strong></strong></a></li>\
                <li><a href="#workloadSummaryPage"> search </a></li></ul>\
         </nav>';
        
        $(selector).append(linkTabs);
        
        // add controller for this view
        function changeTab(event, data) {
            $('#linkTabs li').removeClass("title-tab-selected").addClass("title-tab-hide");
            $('#linkTabs a[href="' + data.id() +'"]').parent().removeClass("title-tab-hide").addClass("title-tab-selected");
        }
        // viewModel -> view control
        vm.subscribe("page", changeTab);
        
        // view -> viewModel control
        $(document).on('click', "#linkTabs li a", function(event){
            vm.page(vmRegistry[this.hash]);
            event.preventDefault();
        });
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
                              <option value="data_pileup" data-search-type="stringMatch">data pileup</option>\
                              <option value="mc_pileup" data-search-type="stringMatch">mc pileup</option>\
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
        
        // change the search options
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
        
        // control submit button
        $(document).on('click', '#WorkloadSummarySearchButton', function(event) {
            var keys = {};
            keys.searchCategory = $('#search_option_board select[name="SearchOptions"] :selected').val();
            keys.searchValue = $('input[name="workloadSummarySearch"]').val();
            vm.SearchPage.keys(keys);
            event.stopPropagation();
        });
    };
    
    return {
        setUTCClock: setUTCClock,
        setLinkTabs: setLinkTabs,
        setWorkloadSummarySearch: setWorkloadSummarySearch
    };
}(jQuery);