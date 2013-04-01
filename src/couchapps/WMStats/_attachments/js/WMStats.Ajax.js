// handles ajax call other than couchdb
WMStats.namespace("Ajax");

WMStats.Ajax = (function($){
    var reqMgrFuncs = {
        putRequest: function(requestArgs) {
            $.ajax("/reqmgr/reqMgr/request", 
                   {type: 'PUT',
                    //accept: {json: "application/json"},
                    //contentType: "application/json",
                    headers: {"Accept": "application/json",
                              "Content-Type": "application/json"},
                    data: JSON.stringify(requestArgs),
                    processData: false,
                    success: function(data, textStatus, jqXHR) {
                            var requestName = data["WMCore.RequestManager.DataStructs.Request.Request"].RequestName;
                            $(WMStats.Globals.Event).triggerHandler(WMStats.CustomEvents.RESUBMISSION_SUCCESS, requestName)
                            },
                    error: function(jqXHR, textStatus, errorThrown){
                            alert(jqXHR.responseText);
                        }
                    });
            
        }
    }
    
    return {
        requestMgr: reqMgrFuncs 
    }
})(jQuery)
