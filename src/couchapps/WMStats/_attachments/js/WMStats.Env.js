WMStats.namespace("Env")
(function(){
    var dataUrl = "/reqmgr/monitorSvc/env";
    $.getJSON(dataUrl, null, 
              function (response) {
                WMStats.Env = response;
              }
             )
})();
