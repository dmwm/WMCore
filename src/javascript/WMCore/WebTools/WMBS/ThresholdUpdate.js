WMCore.namespace("WMBS.ThresholdUpdate")

WMCore.WMBS.ThresholdUpdate.resourceTable = function(divID){

    var textBoxEditorOptions = {
        asyncSubmitter: function(callback, maxSlots){
            var record = this.getRecord(),
                column = this.getColumn(),
                oldValue = this.value,
                datatable = this.getDataTable();
            YAHOO.util.Connect.asyncRequest('GET',
                    '/wmbsservice/wmbs/updatethreshold?siteName=' +
                    record.getData("site") +
                    '&taskType=' + record.getData("type") +
                    '&maxSlots=' + maxSlots, {
                success: function(o){
                
                    alert("change value from " + oldValue + " to " + maxSlots)
                    callback(true, maxSlots)
                },
                failure: function(o){
                    alert("Can't update" + o.statusText);
                    callback();
                },
                scope: this
            });
        },
        disableBtns: true,
        validator: YAHOO.widget.DataTable.validateNumber
    };
    
    var dataSchema = {
        fields: [{
            key: "site"
        }, {
            key: "type"
        }, {
            key: "max_slots",
            label: "max slots",
            editor: new YAHOO.widget.TextboxCellEditor(textBoxEditorOptions)
        }]
    };
    
    var dataUrl = "/wmbsservice/wmbs/listthresholds"
    
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema)
    //writeDebugObject(dataSource)
    //writeEval(dataSource.responseType)
    var dataTable = WMCore.createDataTable(divID, dataSource,
                WMCore.createDefaultTableDef(dataSchema.fields),
                WMCore.createDefaultTableConfig(), 1000000)
    
    
    // Set up editing flow
    var highlightEditableCell = function(oArgs){
        var elCell = oArgs.target;
        if (YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
            this.highlightCell(elCell);
        }
    };
    
    dataTable.subscribe("cellMouseoverEvent", highlightEditableCell);
    dataTable.subscribe("cellMouseoutEvent", dataTable.onEventUnhighlightCell);
    dataTable.subscribe("cellClickEvent", dataTable.onEventShowCellEditor);
}
