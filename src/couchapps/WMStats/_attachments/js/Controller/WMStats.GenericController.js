WMStats.namespace("GenericController");

(function($) {
 // collapsible bar
    $(document).on('click', 'div.caption img', function(event){
        $(this).parent('div.caption').siblings('div.body').toggle('nomal');
    });

    
    $('div').ajaxSend(function(){
            alert("test");
            $(this).show();
        }).live("ajaxComplete", function(){
            $(this).hide();
    });
    
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
})(jQuery)
