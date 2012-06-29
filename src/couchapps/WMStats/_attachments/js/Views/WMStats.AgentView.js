WMStats.namespace("AgentView")

WMStats.AgentView = (function(visFunc) {
    /*
     * create agent view.
     */
    // default couchdb view name to get the agent data
    var _viewName = 'agentInfo';
    // default option
    var _options = {};

    var _visFunc = visFunc || null;
        // campaign summary data
    var _data = null;
    // div id for the view
    var _containerDiv = null;
    
    function setVisualization(visFunc) {
        //visFunc take 2 args (requestData, containerDiv)
        // requestData is instance of WMStatsRequests
        _visFunc = visFunc;
    };
    
    function visualize(data) {
        return _visFunc(WMStats.Agents.setData(data), _containerDiv);
    }
    
   function draw(selector) {
        _containerDiv = selector;
        WMStats.Couch.view(_viewName, _options, visualize)
    }
    
    return {'setVisualization': setVisualization, 'draw': draw};
     
})(WMStats.AgentTable);