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
})(jQuery)
