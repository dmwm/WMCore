WMCore.namespace("GlobalMonitor.SiteMonitor");
/*
 * Overview.js displaces the information gathered from request manager,
 * global queue , local queue and couchDB.
 *
 * All the record are acquired from database on each service (i.e global queue
 * rest service not from the user input).
 */
WMCore.GlobalMonitor.SiteMonitor.statusTable = function(divID){

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

    var dataSchema = {
        fields: [{key: "site_name", label:"Site"},
                 {key: "Pending"},
                 {key: "Running"},
                 {key: "Complete"},
                 {key: "Error"},
                 {key: "success", label: "Success"},
                 {key: "failure", label: "Failure"},
                 {key: "cooloff", label: "Cool Off"},
                 {key: "pending_slots", label:"Job Slots"}]
        };

    var dataUrl = "/reqmgr/monitorSvc/sitemonitor";
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);

    var dataTable = WMCore.createDataTable(divID, dataSource,
                                WMCore.createDefaultTableDef(dataSchema.fields),
                                WMCore.createDefaultTableConfig(), 600000)
};
