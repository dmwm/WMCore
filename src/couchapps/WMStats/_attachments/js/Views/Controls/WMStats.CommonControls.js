WMStats.namespace("CommonControls");
WMStats.CommonControls = function($){
    
    function setUTCClock(selector) {
       setInterval(function() {
            $(selector).text(WMStats.Utils.utcClock());
        }, 100)
    };

    function setLinkTabs(selector) {
       $(selector)
    };
    
    return {
        setUTCClock: setUTCClock,
        setLinkTabs: setLinkTabs
    }
}(jQuery);