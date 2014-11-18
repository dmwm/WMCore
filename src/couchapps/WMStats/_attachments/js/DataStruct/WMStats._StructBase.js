WMStats.namespace("_StructBase");

WMStats._StructBase = function() {
    this._data = null;
};

WMStats._StructBase.prototype = {

    getData: function() {
        return this._data;
    },
    
    setData: function(couchData) {
        this._data = this.convertCouchData(couchData);
    },
    
    getDataByKey: function(key, combineFunc) {
        var data = {};
        var dataList = this._data;
        for (var i in dataList) {
            if (data[dataList[i][key]] === undefined) {
                data[dataList[i][key]] = dataList[i];
            } else if (combineFuc === undefined) {
                data[dataList[i][key]] = dataList[i];
            } else {
                data[dataList[i][key]] = combineFunc(data[dataList[i][key]], dataList[i]);
            }
            
        }
        return data;
    },
    
    convertCouchData: function(couchData) {
        // defaut conversion need to be over written
        return couchData;
    }
};
