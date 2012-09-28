WMStats.namespace("_ModelBase")

WMStats._ModelBase = function(initView, options, dataStruct) {

    this._initialView = initView;
    this._options = options;
    this._dataStruct = dataStruct;
    this._trigger = null;
    this._data = null;
}

WMStats._ModelBase.prototype = {

    setTrigger: function(triggerName) {
        this._trigger = triggerName;
    },
    
    getData: function() {
        return this._data;
    },
    
    dataReady: function(data) {
        this._data = this._dataStruct(data);
        jQuery(WMStats.Globals.Event).triggerHandler(this._trigger, this._data)
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

