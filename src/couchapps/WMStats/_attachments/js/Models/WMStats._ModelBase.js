WMStats.namespace("_ModelBase");

WMStats._ModelBase = function(initView, options, dataStruct) {

    this._initialView = initView;
    this._options = options;
	this._dataStruct = dataStruct;
    this._trigger = null;
    this._data = null;
    // default _dbSource is wmstats database
    this._dbSource = WMStats.Couch;
};

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
                jQuery(WMStats.Globals.Event).triggerHandler(this._trigger[i], this._data);
            }
        }else{
            jQuery(WMStats.Globals.Event).triggerHandler(this._trigger, this._data);
        }
        
    },

    retrieveData: function (view, options) {
        if (options === undefined){
            var options = this._options;
        }
        if (view === undefined) {
            var view = this._initialView;
        }
        
        if (view === "allDocs") {
            return this.retrieveAllDocs(options);
        } else if (view === "doc") {
            return this.retrieveDoc(options);
        }else {
            return this._dbSource.view(view, options, 
                               jQuery.proxy(this.callback, this));
        }
       
    },
    
    retrieveAllDocs: function (options) {
        if (options === undefined){
            var options = this._options;
        }
        return this._dbSource.allDocs(options, jQuery.proxy(this.callback, this));
    },
    
    retrieveDoc: function (docID) {
        return this._dbSource.openDoc(docID, jQuery.proxy(this.callback, this));
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
