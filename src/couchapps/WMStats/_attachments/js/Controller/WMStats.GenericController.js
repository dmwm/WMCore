WMStats.namespace("GenericController");

(function($) {
 // collapsible bar
    function closeRequestDetail() {
        $("#request_view div.detail_data").hide('puff', {}, 500);
        WMStats.Env.RequestDetailOpen = false;
    };
    
    $(document).on('click', 'div.caption img', function(event){
        $(this).parent('div.caption').siblings('div.body').toggle('nomal');
    });

    $(document).on('click', 'div.closingButton', function(event){
        //$(this).parent('div').hide('puff', {}, 500);
        //TODO: generalize to all the button not just for request detail
        closeRequestDetail();
        event.preventDefault();
    });
   
    $(document).keyup(function(event) {
        if (WMStats.Env.RequestDetailOpen && event.keyCode == 27) {
            closeRequestDetail();
            event.preventDefault();
        }
    })

    WMStats.GenericController.switchView = function (showSelector, hideSelectors) {
        if (!showSelector) {
            showSelector = WMStats.Env.View;
        } else if (!hideSelectors) {
            var viewList = ["#category_view", "#request_view", "#job_view"];
            for (var i in viewList) {
                if (showSelector != viewList[i]){
                    $(viewList[i]).hide();
                }
                
            }
        } else {
            for (var i in hideSelectors) {
                $(hideSelectors[i]).hide();
            }
        }
        $(showSelector).show();
        WMStats.Env.View = showSelector;
        // select the tab
        $('#tab_board li').removeClass("tabs-selected");
        $('#tab_board a[href="' + showSelector +'"]').parent().addClass("tabs-selected")
    };
    
    WMStats.GenericController.switchPage = function (showSelector, hideSelectors) {
        if (!showSelector) {
            showSelector = WMStats.Env.Page;
        } else if (!hideSelectors) {
            var pageList = ["#activeRequestPage", "#agentInfoPage", "#workloadSummaryPage"];
            for (var i in pageList) {
                if (showSelector != pageList[i]){
                    $(pageList[i]).hide();
                }
            }
        } else {
            for (var i in hideSelectors) {
                $(hideSelectors[i]).hide();
            }
        }
        $(showSelector).show();
        WMStats.Env.Page = showSelector;
        // select the tab
        $('#linkTabs li').removeClass("title-tab-selected").addClass("title-tab-hide");
        $('#linkTabs a[href="' + showSelector +'"]').parent().removeClass("title-tab-hide").addClass("title-tab-selected")
    };
    
    $(document).on('click', "#linkTabs li a", function(event){
        WMStats.GenericController.switchPage(this.hash);
        event.preventDefault();
    });
    
})(jQuery);
