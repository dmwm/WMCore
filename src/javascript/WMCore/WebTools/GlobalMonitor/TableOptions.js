WMCore.namespace("GlobalMonitor.TableOptions")

WMCore.GlobalMonitor.TableOptions.columnControl = function(myDataTable, dialogDiv, optionLink, scope){

    // Shows dialog, creating one when necessary
    var newCols = true;
    var showDlg = function(e){
        YAHOO.util.Event.stopEvent(e);

        if (newCols) {
            // Populate Dialog
            // Using a template to create elements for the SimpleDialog
            var allColumns = myDataTable.getColumnSet().keys;
            var elPicker = YAHOO.util.Dom.get("dt-dlg-picker");
            var elTemplateCol = document.createElement("div");
            YAHOO.util.Dom.addClass(elTemplateCol, "dt-dlg-pickercol");
            var elTemplateKey = elTemplateCol.appendChild(document.createElement("span"));
            YAHOO.util.Dom.addClass(elTemplateKey, "dt-dlg-pickerkey");
            var elTemplateBtns = elTemplateCol.appendChild(document.createElement("span"));
            YAHOO.util.Dom.addClass(elTemplateBtns, "dt-dlg-pickerbtns");
            var onclickObj = {
                fn: handleButtonClick,
                obj: this,
                scope: false
            };

            // Create one section in the SimpleDialog for each Column
            var elColumn, elKey, elButton, oButtonGrp;
            for (var i = 0, l = allColumns.length; i < l; i++) {
                var oColumn = allColumns[i];

                // Use the template
                elColumn = elTemplateCol.cloneNode(true);

                // Write the Column key
                elKey = elColumn.firstChild;
                elKey.innerHTML = oColumn.getKey();

                // Create a ButtonGroup
                oButtonGrp = new YAHOO.widget.ButtonGroup({
                    id: "buttongrp" + i,
                    name: oColumn.getKey(),
                    container: elKey.nextSibling
                });
                oButtonGrp.addButtons([{
                    label: "Show",
                    value: "Show",
                    checked: ((!oColumn.hidden)),
                    onclick: onclickObj
                }, {
                    label: "Hide",
                    value: "Hide",
                    checked: ((oColumn.hidden)),
                    onclick: onclickObj
                }]);

                elPicker.appendChild(elColumn);
            }
            newCols = false;
        }
        myDlg.show();
    };

    var hideDlg = function(e){
        this.hide();
    };
    var handleButtonClick = function(e, oSelf){
        var sKey = this.get("name");
        if (this.get("value") === "Hide") {
            // Hides a Column
            myDataTable.hideColumn(sKey);
            //var column = myDataTable.getColumn(sKey);
            //var cells = YAHOO.util.Selector.query('.yui-dt-col-' + column.key, myDataTable.getTableEl());
            //YAHOO.util.Dom.addClass(cells,'yui-dt-hidden');
        }
        else {
            // Shows a Column
            myDataTable.showColumn(sKey);
            //var column = myDataTable.getColumn(sKey);
            //var cells = YAHOO.util.Selector.query('.yui-dt-col-' + column.key, myDataTable.getTableEl());
            //YAHOO.util.Dom.removeClass(cells,'yui-dt-hidden');
        }
    };

    // Create the SimpleDialog
    YAHOO.util.Dom.removeClass(dialogDiv, "inprogress");
    var myDlg = new YAHOO.widget.SimpleDialog(dialogDiv, {
        width: "30em",
        visible: false,
        modal: true,
        buttons: [{
            text: "Close",
            handler: hideDlg
        }],
        fixedcenter: true,
        constrainToViewport: true
    });
    myDlg.render();

    // Nulls out myDlg to force a new one to be created
    myDataTable.subscribe("columnReorderEvent", function(){
        newCols = true;
        YAHOO.util.Event.purgeElement("dt-dlg-picker", true);
        YAHOO.util.Dom.get("dt-dlg-picker").innerHTML = "";
    }, this, true);

    // Hook up the SimpleDialog to the link
    YAHOO.util.Event.addListener(optionLink, "click", showDlg, this, true);

}
