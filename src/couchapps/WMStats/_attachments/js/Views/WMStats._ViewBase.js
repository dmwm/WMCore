WMStats.namespace("_ViewBase")

WMStats._ViewBase = function(initView, options, dataStruct, visFunc) {

    this._initialView = initView;
    this._options = options;
    this._dataStruct = dataStruct;
    this._visFunc = visFunc || null;
    this._containerDiv = null;
    this._data = null;
}

WMStats._ViewBase.prototype = {

    setVisualization: function(visFunc) {
        //visFunc take 2 args (requestData, containerDiv)
        // requestData is instance of WMStatsRequests
        this._visFunc = visFunc;
    },
    
    getData: function() {
        return this._data;
    },
    
    visualize: function(data) {
        this._data = this._dataStruct(data);
        return this._visFunc(this._data, this._containerDiv);
    },
    
    draw: function(selector, cache) {
        this._containerDiv = selector;
        if (cache && this._data) {
            return this._visFunc(this._data, this._containerDiv);
        } else {
            return WMStats.Couch.view(this._initialView, this._options, 
                               jQuery.proxy(this.callback, this))
        }
    },
    
    callback: function (data) {
        // use current object context
        return this.visualize(data);
    },
    
    clearData: function () {
        delete this._data;
    }
}

