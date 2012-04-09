WMStats.namespace("AlertView")

WMStats.AlertView = (function() {
    
    var _data = null;
    var _containerDiv = null;
    var _viewName = 'cooledoffRequests';
    var _options = {"group_level": 1, "reduce": true};
    var _tableID = "alertTable";
    
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "request"},
            { "mDataProp": "count", "sTitle": "jobs"}
        ]
    }
    
    
    function setAlertData(data) {
        var rows =[]
        for (var i in data) {
            var tableRow = {};
            tableRow.workflow = data[i].key;
            tableRow.count = data[i].value;
            rows.push(tableRow)
        }
        _data = rows;
    }
    
    function createAlertTable(data) {
        setAlertData(data.rows);
        tableConfig.aaData = _data;
        var selector =  _containerDiv + " table#" + _tableID;
        return WMStats.Table(tableConfig).create(selector)
    }
    
    function createTable(selector, options){
        if (options) {_options = options;}
        _containerDiv = selector;
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="' + _tableID + '"></table>' );
        WMStats.Couch.view(_viewName, _options, createAlertTable)
    }
    
    return {'createTable': createTable};    
})();

