WMStats.namespace("Controls");
WMStats.Controls = function($){
    var _filterSelector;
    var _categorySelector;
    var vm =  WMStats.ViewModel;
    var vmRegistry = WMStats.ViewModel.Registry;
    
    function setFilter(selector) {
        var inputFilter = '<div name="filter">\
                           workflow: <input name="workflow" value=""></input>\
                           status: <input name="request_status" value=""></input>\
                           run: <input name="run" value=""></input>\
                           </div>';
        $(selector).append(inputFilter);
        _filterSelector = selector + ' div[name="filter"] input';
        
        $(document).on('keyup', selector + " input", 
                function() { WMStats.Utils.delay(function() {
                    //change the view model filter value
                    WMStats.ViewModel.ActiveRequestPage.filter(WMStats.Utils.createInputFilter(_filterSelector));
                    
                }, 300)});
    }
    
    function setCategoryButton(selector) {

        vm.CategoryView.category(vm.RunCategory);
    };
    
    function setViewSwitchButton(selector) {
       var viewSwitchBottons = 
        '<nav id="view_switch_button" class="button-group">\
            <ul><li><a href="#progress" class="nav-button nav-button-selected"> progress </a></li>\
         </nav>';
        
        $(selector).append(viewSwitchBottons);

        $(document).on('click', "#view_switch_button li a", function(event){
            // format is either progress or numJobs need to decouple the name
            var buttonName = this.hash.substring(1);
            var requestFormat;
            if (buttonName === "progress") {
                requestFormat = vm.RequestProgress;
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
    
    function getFilter() {
        return WMStats.Utils.createInputFilter(_filterSelector);
    };
    
    
    function setTabs(selector) {
        var tabs = '<ul><li class="first"><a href="#category_view">Run</a></li>\
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
            //vm.CategoryView.category(vm.RunCategory);
            event.preventDefault();
        });
    };
    
    return {
        setFilter: setFilter,
        setTabs: setTabs,
        setCategoryButton: setCategoryButton,
        setAllRequestButton: setAllRequestButton,
        setViewSwitchButton: setViewSwitchButton,
        getFilter: getFilter,
        requests: "requests",
        sites: "sites",
        run: "run"
    };
}(jQuery);
