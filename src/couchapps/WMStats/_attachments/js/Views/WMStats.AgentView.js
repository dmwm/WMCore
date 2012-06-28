WMStats.namespace("AgentView")

WMStats.AgentView = (function() {
    /*
     * create campaign table view.
     */
    // campaign summary data
    var _data = null;
    // div id for the view
    var _containerDiv = null;
    // default couchdb view name to get the campaign dat
    var _viewName = 'agentInfo';
    // default option
    var _options = {};
    // id for the table.
    var _tableID = "agentTable";
    
    // jquery datatable config
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "agent_url", "sTitle": "agent url",
              "fnRender": function ( o, val ) {
                            return decodeURIComponent(o.aData.agent_url);
                      }
            },               
            { "mDataProp": "status", "sTitle": "status"},
            { "mDataProp": "agent_team", "sTitle": "teams"},
            { "mDataProp": "down_components", "sTitle": "components down", 
              "sDefaultContent": ""}
        ]
    }
    
    function getData() {
        return _data;
    }
    
    
    function setAgentData(data) {
        var rows =[]
        for (var i in data) {
            var tableRow = data[i].value;
            rows.push(tableRow)
        }
        _data = rows;
        return rows
    }
    
    function createAgentTable(data) {
        setAgentData(data.rows);
        tableConfig.aaData = _data;
        var selector =  _containerDiv;
        return WMStats.Table(tableConfig).create(selector)
    }
    
   function createTable(selector) {
        _containerDiv = selector;
        //$(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="'+ _tableID + '"></table>' );
        WMStats.Couch.view(_viewName, _options, createAgentTable)
    }
    
    return {'getData': getData, 'createTable': createTable};
     
})();