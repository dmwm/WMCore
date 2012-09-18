WMStats.namespace("AgentStatusGUI");

WMStats.AgentStatusGUI = function (data, containerDiv) {
    var currentTime = Math.round(new Date().getTime() / 1000);
    var dataList = data.getData();
    var collectiveStatus = "ok";
    
    function getStatus(agentInfo) {
        if (currentTime - agentInfo.timestamp > 600) {return "agent_down"};
        if (agentInfo.down_components.length > 0) {return "component_down"};
        return "ok";
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
        var status = getStatus(dataList[index]);
        if (status == "agent_down") {
            setStatus("error");
            htmlList += ('<li>' + dataList[index].agent_url + '</li>');
        } else if (status == "component_down") {
            setStatus("warning");
            htmlList += ('<li>' + dataList[index].agent_url + ': ' + 
                          dataList[index].down_components +'</li>');
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
}