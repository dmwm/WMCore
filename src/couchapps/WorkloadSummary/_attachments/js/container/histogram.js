/***
 * convertHistogramData
 * takes data which get performanceDataByCampaign view  
 * and construct the list of by campaign and workload (workflow)
 * (check data format from view/performaceDataByCampaign/map.js 
 * @param {Object} data
 */
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

/***
 * 
 * @param {Object} data
 * 
 * TODO: doesn't handle the underflow and overflow correctly.
 * need to find what operators wants. commentted out the code
 */
convertToPVHistoData = function(data) {
    var convertedData = {};
    convertedData.lowerbound = 0;
    convertedData.upperbound = 0;
    convertedData.bins = [];
    for (var i in data) {
        bin = {};
        var upperBound = 0;// need to set correctly for overflow
        switch (data[i].type) {
            case "underflow":
                //bin.x = 0; // need to handle underflow
                //bin.dx = 10;
                break;
            case "overflow":
                //bin.x = 100; // need to handle overflow
                //bin.dx = 10;
                //upperBound = 110;
                break;
            case "standard":
                // minus value shouldn't happen but if there is 
                // the case mapped to 0 to draw the histogram correctly.
                if (data[i].lowerEdge < 0){
                    data[i].lowerEdge = 0;
                };
                if (data[i].upperEdge < 0){
                    data[i].upperEdge = 0;
                };
                bin.x = data[i].lowerEdge;
                bin.dx = data[i].upperEdge - data[i].lowerEdge;
                upperBound = data[i].upperEdge;
                break;
            default:
        }
        bin.y = data[i].nEvents;
        convertedData.bins[i] = bin;
        // it is not assumed data is ordered so take the max
        convertedData.upperbound = Math.max(convertedData.upperbound, upperBound)
    };
    return convertedData;
};


histogramContainer = function(rootPanel, config, data, category){
    var histData = convertToPVHistoData(data);
    var w = config.width, 
        h = config.height, 
        x = pv.Scale.linear(histData.lowerbound, histData.upperbound).range(0, w), 
        y = pv.Scale.linear(0, pv.max(histData.bins, function(d){
        return d.y
    })).range(0, h - 20);
    
    var vis;
    if (!rootPanel) {
        vis = new pv.Panel();
    }else {
        vis = rootPanel.add(pv.Panel);
    };

    vis.width(w)
       .height(h)
       .top(config.top + config.hPadding)
       .left(config.left + config.wPadding);
    
   if (config.label) {
        var label = vis.add(pv.Label);
        //set default label position
        label.top(10).left(100).textAlign("center").text(category);
        
        for (prop in config.label) {
            label[prop](config.label[prop]);
        }
    }

    vis.add(pv.Bar)
        .data(histData.bins)
        .bottom(0)
        .left(function(d){return x(d.x)})
        .width(function(d){return x(d.dx);})
        .height(function(d){return y(d.y);})
        .fillStyle("rgb(169, 169, 169)")
        .strokeStyle("rgba(255, 255, 255, .2)")
        .lineWidth(1)
        .antialias(false)
       .anchor('top').add(pv.Label)
          .text(function(d){return d.y})
          .textStyle("blue")
          .font("9px sans-serif")
          .textBaseline("bottom")
          .visible(function(d){return (d.y > 5)});
    
    vis.add(pv.Rule)
        .data(y.ticks(5))
        .bottom(y)
        .width(w)
        .left(0)
        .strokeStyle("#fff")
        .anchor("left")
        .add(pv.Label)
        .text(y.tickFormat);
    
    vis.add(pv.Rule)
        .data(x.ticks())
        .left(function(d){return x(d)})
        .bottom(-5)
        .height(5)
      .anchor("bottom").add(pv.Label)
        .text(x.tickFormat);

    vis.add(pv.Rule)
        .bottom(0);
        
    return vis
}