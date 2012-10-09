WMStats.namespace("AgentStatusGUI");

WMStats.AgentStatusGUI = function (data, containerDiv) {
    var currentTime = Math.round(new Date().getTime() / 1000);
    var dataList = data.getData();
    var collectiveStatus = "ok";
    var agentPollingCycle = 600;
    
    function getStatus(agentInfo) {
        var lastUpdatedDuration = currentTime - agentInfo.timestamp;
        if (lastUpdatedDuration > agentPollingCycle) {
            return {staus: "agent_down", 
                    message: WMStats.Utils.foramtDuration(lastUpdatedDuration)};
        };
        if (agentInfo.down_components.length > 0) {
            return {staus: "component_down",
                    message: agentInfo.down_components};
        };
        return {status: "ok", 
                message: WMStats.Utils.foramtDuration(lastUpdatedDuration)};
    };
    function setStatus(status) {
        if (collectiveStatus == "ok") {
            collectiveStatus = status;
        } else if (collectiveStatus == "warning" && status == "error") {
            collectiveStatus = "error";
        }
    }
    var htmlList = "<ul>";
    
    for (var index in dataList) {
        var statusInfo = getStatus(dataList[index]);
        if (statusInfo.staus == "agent_down") {
            setStatus("error");
        } else if (statusInfo.staus == "component_down") {
            setStatus("warning");
        }
        htmlList += ('<li>' + dataList[index].agent_url + ": " + 
                              statusInfo.message +'</li>');
        
    }
    
    htmlList += "</ul>";
    
    if (collectiveStatus == "ok") {
        $(containerDiv).removeClass("warning error").addClass("stable");
    } else if (collectiveStatus == "warning") {
       $(containerDiv).removeClass("stable error").addClass("warning").html(htmlList);
    } else if (collectiveStatus == "error") {
        $(containerDiv).removeClass("stable warning").addClass("error").html(htmlList);
    }
}