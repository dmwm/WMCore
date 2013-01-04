WMStats.namespace("_ModelBase")

WMStats._ModelBase = function(initView, options, dataStruct) {

    this._initialView = initView;
    this._options = options;
    this._dataStruct = dataStruct;
    this._trigger = null;
    this._data = null;
    this._dbSource = WMStats.Couch;
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
        if (this._trigger instanceof Array){
            for (var i in this._trigger) {
                jQuery(WMStats.Globals.Event).triggerHandler(this._trigger[i], this._data)
            }
        }else{
            jQuery(WMStats.Globals.Event).triggerHandler(this._trigger, this._data)
        }
        
    },

    retrieveData: function () {
        return this._dbSource.view(this._initialView, this._options, 
                               jQuery.proxy(this.callback, this))
    },
    
    retrieveAllDocs: function () {
        return this._dbSource.allDocs(this._options, 
                               jQuery.proxy(this.callback, this))
    },
    
    setDBSource: function(dbSource) {
        this._dbSource = dbSource;
    },
    callback: function (data) {
        // use current object context
        return this.dataReady(data);
    },
    
    clearData: function () {
        delete this._data;
    }
};
