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
        if (!hideSelectors) {
            $("#category_view").hide();
            $("#request_view").hide();
            $("#job_view").hide();
        } else {
            for (var i in hideSelectors) {
                $(hideSelectors[i]).hide();
            }
        }
        $(showSelector).show();
    }

})(jQuery)
