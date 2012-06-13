WMStats.namespace("ActiveRequestView");
(function() {
    var initView = 'requestByStatus'; 
    var options = {'keys': ['new', 'approved', 'assigned', 'negotiating', 
                            'acquired', 'running', 'completed'], 
                   'include_docs': true};
    WMStats.ActiveRequestView =  new WMStats._RequestViewBase(initView, options);
})()
