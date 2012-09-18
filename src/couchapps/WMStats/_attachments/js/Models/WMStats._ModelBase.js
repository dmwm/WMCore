WMStats.namespace("_ModelBase")

WMStats._ModelBase = function(initView, options, dataStruct, visFunc) {

    this._initialView = initView;
    this._options = options;
    this._dataStruct = dataStruct;
    this._visFunc = visFunc || null;
    this._trigger = null;
    this._data = null;
}

WMStats._ModelBase.prototype = {

    setVisualization: function(visFunc) {
        //visFunc take 2 args (requestData, containerDiv)
        // requestData is instance of WMStatsRequests
        this._visFunc = visFunc;
    },
    
    setTrigger: function(triggerName) {
        this._trigger = triggerName;
    },
    
    getData: function() {
        return this._data;
    },
    
    dataReady: function(data) {
        this._data = this._dataStruct(data);
        jQuery(WMStats.Globals.Event).triggerHandler(this._trigger, this._data)
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
    
    retrieveData: function () {
        return WMStats.Couch.view(this._initialView, this._options, 
                               jQuery.proxy(this.callback, this))
    },
    
    callback: function (data) {
        // use current object context
        return this.dataReady(data);
    },
    
    clearData: function () {
        delete this._data;
    }
}

