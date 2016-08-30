WMStats.namespace("_AjaxModelBase");

WMStats._AjaxModelBase = function(uri, dataStruct) {
	this._uri = uri;
	this._dataStruct = dataStruct;
    this._data = null;
    // default _dbSource is wmstats database
};

WMStats._AjaxModelBase.prototype = {
	
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
    
	retrieveData: function(args) {
            $.ajax(this._uri, 
                   {type: 'GET',
                    //accept: {json: "application/json"},
                    //contentType: "application/json",
                    headers: {"Accept": "application/json"},
                    processData: false,
                    data: args || "",
                    success: jQuery.proxy(this.callback, this),
                    error: function(jqXHR, textStatus, errorThrown){
                            console.log("call fails, response: " + jqXHR.responseText);
                        }
                    });
    },
  
    callback: function (data, textStatus, jqXHR) {
        // use current object context
        return this.dataReady(data);
    },
    
    clearData: function () {
        delete this._data;
    }
};
