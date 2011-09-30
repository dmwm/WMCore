WMCore.namespace("GlobalMonitor.Env");
(function(){
    var callbak = {
        success: function (o) {WMCore.GlobalMonitor.Env = YAHOO.lang.JSON.parse(o.responseText);}
    };
    var dataUrl = "/reqmgr/monitorSvc/env";
    //Always send accept type as application/json
    YAHOO.util.Connect.initHeader("Accept", "application/json", true);
    var request = YAHOO.util.Connect.asyncRequest('GET', dataUrl, callback);
})();
