WMStats.namespace("AgentStatusGUI");

WMStats.AgentStatusGUI = function (data, containerDiv) {
    var dataList = data.getAlertList();
    var collectiveStatus = "ok";
   
    function setStatus(status) {
        if (collectiveStatus == "ok") {
            collectiveStatus = status;
        } else if (collectiveStatus == "warning" && status == "error") {
            collectiveStatus = "error";
        }
    }
    var htmlList = "<ul>";
    
    for (var index in dataList) {
        var statusInfo = dataList[index].alert;
        if (statusInfo.status == "agent_down") {
            setStatus("error");
            htmlList += ('<li>' + dataList[index].agent_url + ": " + 
                              statusInfo.message +'</li>');
        } else if (statusInfo.status == "component_down") {
            setStatus("warning");
            htmlList += ('<li>' + dataList[index].agent_url + ": " + 
                              statusInfo.message +'</li>');
        }
    }
    
    htmlList += "</ul>";
    
    if (collectiveStatus == "ok") {
        $(containerDiv).removeClass("warning error").addClass("stable");
    } else if (collectiveStatus == "warning") {
       $(containerDiv).removeClass("stable error").addClass("warning").html(htmlList);
    } else if (collectiveStatus == "error") {
        $(containerDiv).removeClass("stable warning").addClass("error").html(htmlList);
    }
};
