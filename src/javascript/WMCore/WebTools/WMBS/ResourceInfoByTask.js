WMCore.namespace("WMBS.ResourceInfoByTask");

WMCore.WMBS.ResourceInfoByTask.resourceInfo = function(divID){

    YAHOO.util.Event.onDOMReady(function(){
    
        // shortnames
        var Dom = YAHOO.util.Dom,
            Event = YAHOO.util.Event,
            Lang = YAHOO.lang,
            REDT = YAHOO.widget.RowExpansionDataTable, // property name within the expansion state for my own data
            NESTED_DT = 'nestedDT';
        
        var taskSchema = {
            fields: [{
                key: "task"
            }, {
                key: "running"
            }]
        };
        
        var taskUrl = "/wmbsservice/wmbs/listtaskbysite"
        
        var taskSource = WMCore.createDataSource(taskUrl, taskSchema)
        
        
        // Function to show the tracks of a given album
        // It takes the expansion state object of the containing row
        var showTasks = function(state){
            var taskDT = new REDT(state.expLinerEl, taskSchema.fields, taskSource, {
                initialRequest: "?siteName=" + state.record.getData('site') +
                                 "&taskType=" + state.record.getData('task_type')
            });
            
            // Store the reference to this datatable object for any further use 
            this.setExpansionState(state.record, NESTED_DT, taskDT);
        }
        
        var thresholdSchema = {
            fields: [{
                key: "site"
            }, {
                key: "pending_slots"
            }, {
                key: "running_slots"
            }, {
                key: "max_slots"
            }, {
                key: "task_type"
            }, {
                key: "task_pending_jobs"
            }, {
                key: "task_running_jobs"
            }]
        };
        
        var thresholdCol = [{
            key: "task_type",
            label: "type"
        }, {
            key: "max_slots",
            label: "threshold"
        }, {
            key: "task_running_jobs",
            label: "running"
        }]
        
        var thresholdUrl = "/wmbsservice/wmbs/thresholdbysite"
        
        var thresholdSource = WMCore.createDataSource(thresholdUrl, thresholdSchema)
        
        var showThresholds = function(state){
            var thresholdDT = new REDT(state.expLinerEl, thresholdCol, thresholdSource, {
            
                initialRequest: "?site=" + state.record.getData('site'),
                // On row expansion, I will call showTasks
                rowExpansionTemplate: showTasks
            });
            
            // Store the reference to this datatable object for any further use 
            // (specially destroying it, as above)
            this.setExpansionState(state.record, NESTED_DT, thresholdDT);
            
            //Subscribe to the rowExpansionDestroyEvent so I can destroy the taskDT table
            // before its container (the album row) is gone and it ends a zombie
            thresholdDT.on('rowExpansionDestroyEvent', function(state){
                state[NESTED_DT].destroy();
            });
        };
        
        
        var siteSchema = {
            fields: [{
                key: "site"
            }, {
                key: "total_slots"
            }, {
                key: "pending_jobs",
                label: "jobs in wmagents"
            }]
        };
        
        var siteUrl = "/wmbsservice/wmbs/listthresholdsforcreate/"
        
        var siteSource = WMCore.createDataSource(siteUrl, siteSchema)
        
        
        var siteDT = new REDT(divID, siteSchema.fields, siteSource, {
        
            // On row expansion, I will call showThresholds
            rowExpansionTemplate: showThresholds
        
        });
        
        //Subscribe to the rowExpansionDestroyEvent so I can destroy the thresholdDT
        // before its container (the artist row) is gone and it ends a zombie
        siteDT.on('rowExpansionDestroyEvent', function(state){
            state[NESTED_DT].destroy();
        });
    });
}
