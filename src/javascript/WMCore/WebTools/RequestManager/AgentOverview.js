WMCore.namespace("RequestManager.AgentOverview");
/*
 * Overview.js displaces the information gathered from request manager,
 * global queue , local queue and couchDB.
 *
 * All the record are acquired from database on each service (i.e global queue
 * rest service not from the user input).
 */
WMCore.RequestManager.AgentOverview.statusTable = function(divID){

    var formatAgentLink = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (!sData) {
                elCell.innerHTML = "Not Available";
            } else {
                host = sData.split('/')[2];
                elCell.innerHTML = "<a href='" + sData  + "monitor' target='_blank'>" +
                                     host + "</a>";
            };
        };

    var formatACDCLink = function(elCell, oRecord, oColumn, sData) {
            if (!sData) {
                elCell.innerHTML = "Not Available";
            } else {
                elCell.innerHTML = "<a href='" + sData  + "' target='_blank'> acdc </a>";
            };
        };
    
    var formatStatus = function(elCell, oRecord, oColumn, sData) {
            if (sData == "down") {
                elCell.innerHTML = "<font color='red'> One or more componts down </font>";
            } else {
                elCell.innerHTML = sData;
            };
        };

    var dataSchema = {
        fields: [{key: "url", label:"Agent Location", formatter:formatAgentLink},
                 {key: "status", formatter: formatStatus},
                 {key: "acdc", formatter:formatACDCLink}]
        };

    var dataUrl = "/reqmgr/reqMgr/agentoverview";
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);

    var dataTable = WMCore.createDataTable(divID, dataSource,
                                WMCore.createDefaultTableDef(dataSchema.fields),
                                WMCore.createDefaultTableConfig(), 100000)
};
