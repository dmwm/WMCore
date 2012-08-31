WMStats.namespace("ActiveRequestView");
WMStats.ActiveRequestView = function() {
    var initView = 'requestByStatus'; 
    var options = {'keys': [
                            "new",
                            //"testing-approved",
                            //"testing",
                            //"tested",
                            //"test-failed",
                            "assignment-approved",
                            "assigned",
                            "ops-hold",
                            "negotiating",
                            "acquired",
                            "running",
                            "failed",
                            "epic-FAILED",
                            "completed",
                            "closed-out",
                            "announced",
                            "aborted",
                            "rejected"
                            ], 
                   'include_docs': true};
    var reqView = new WMStats._RequestViewBase(initView, options, 
                                       WMStats.ActiveRequestTable);
    reqView.setTrigger('activeRequestReady');
    return reqView;
}()
