WMCore.namespace("WorkQueue.ElementInfoByWorkflow")

WMCore.WorkQueue.ElementInfoByWorkflow.elementTable = function(args) {
        
    var formatUrl = function(elCell, oRecord, oColumn, sData) { 
            var postfixLink = "/template/ElementSummaryByWorkflow?workflow=";
			var host;
            if (!sData) {
                host = sData;
            } else {
                host = sData.split('/')[2]
            }
            elCell.innerHTML = "<a href='" + sData + "monitor" + 
                                 postfixLink + oRecord.getData("spec_name") +
                                 "' target='_blank'>" + host + "</a>"; 
        };

    var percentFormat = function(elCell, oRecord, oColumn, sData) { 
            if (!sData) {
                percent = 0;
            } else {
                percent = sData
            }
            elCell.innerHTML =  sData + "%"; 
        };
        
    var dateFormatter = function(elCell, oRecord, oColumn, oData) {
        
        var oDate = new Date(oData*1000);
        //for the formatting check 
        // http://developer.yahoo.com/yui/docs/YAHOO.util.Date.html#method_format
        var str = YAHOO.util.Date.format(oDate, { format:"%D %T"});
        elCell.innerHTML = str;
    }
        
    var dataSchema = {
        fields: [{key: "id"}, {key: "spec_name"}, {key: "task_name"}, {key: "element_name"}, 
                 {key: "status"}, {key: "child_queue", formatter:formatUrl}, 
                 //{key: "parent_flag"},
                 {key: "priority"}, {key: "num_jobs", label: "jobs"},
                 //{key: "parent_queue_id"}, 
                 //{key: "subscription_id", label: "sub id"},
                 {key: "team_name"},
                 {key: "events_written", label: "events"}, 
                 {key: "files_processed", label: "file"},
                 {key: "percent_complete", label: "complete", formatter:percentFormat}, 
                 {key: "percent_success", label: "success", formatter:percentFormat},
                 {key: "insert_time", formatter:dateFormatter},
                 {key: "update_time", formatter:dateFormatter}
                ]
        };

    var dataUrl = "/workqueue/elementsbyworkflow/" + args.workflow

    var dataSource = WMCore.createDataSource(dataUrl, dataSchema)
    
    var tableConfig = WMCore.createDefaultTableConfig("id");
    
    tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 10});
    
    var dataTable = WMCore.createDataTable(args.divID, dataSource, 
                         WMCore.createDefaultTableDef(dataSchema.fields),
                         tableConfig, 50000);
}