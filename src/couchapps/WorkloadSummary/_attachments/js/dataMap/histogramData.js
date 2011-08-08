convertToHistogramData = function(data){
    var histoDataCollection = new Array();
    for (var i in data.rows) {
        for (var stat in data.rows[i].value) {
            var item = {};
            item['campaign'] = data.rows[i].key[0];
            item['workload'] = data.rows[i].key[1];
            item['histoData'] = histoData(data.rows[i].value[stat]);
            histoDataCollection.push(item);
        }
    };
    return histoDataCollection;
};

histoData = function(data) {
    var convertedData = {}
    convertedData.lowerbound = 0;
    convertedData.upperbound = 0;
    convertedData.bins = []
    for (var i in data) {
        bin = {};
        bin.x = data[i].lowerEdge;
        bin.dx = data[i].upperEdge - data[i].lowerEdge;
        bin.y = data[i].nEvents;
        convertedData.bins[i] = bin;
        // it is not assumed data is ordered so take the max
        convertedData.upperbound = Math.max(convertedData.upperbound, data[i].upperEdge)
    };
    return convertedData;
};
