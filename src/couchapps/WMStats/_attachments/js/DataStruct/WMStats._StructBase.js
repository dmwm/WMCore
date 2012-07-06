WMStats.namespace("_StructBase")

WMStats._StructBase.prototype = {

    getData: function() {
        return _data;
    },
    
    setData: function(data) {
        _data = data
    }
}
