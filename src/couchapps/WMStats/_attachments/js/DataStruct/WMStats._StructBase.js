WMStats.namespace("_StructBase")

WMStats._StructBase = function() {
    this._data = null;
}

WMStats._StructBase.prototype = {

    getData: function() {
        return this._data;
    },
    
    setData: function(couchData) {
        this._data = this.convertCouchData(couchData)
    }
};
