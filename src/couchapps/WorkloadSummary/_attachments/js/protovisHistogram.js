pHistogram = function (config, histData) {

    var w = config.width,
        h = config.height,
        x = pv.Scale.linear(histData.lowerbound, histData.upperbound).range(0, w),
        y = pv.Scale.linear(0, pv.max(histData.bins, function(d){return d.y})).range(0, h);
    
    var vis = new pv.Panel()
        .width(w)
        .height(h)
        .margin(20);
    
    vis.add(pv.Bar)
        .data(histData.bins)
        .bottom(0)
        .left(function(d) {return x(d.x)})
        .width(function(d) {return x(d.dx)})
        .height(function(d){return y(d.y)})
        .fillStyle("#aaa")
        .strokeStyle("rgba(255, 255, 255, .2)")
        .lineWidth(1)
        .antialias(false);
    
    vis.add(pv.Rule)
        .data(y.ticks(5))
        .bottom(y)
        .strokeStyle("#fff")
      .anchor("left").add(pv.Label)
        .text(y.tickFormat);
    
    vis.add(pv.Rule)
        .data(x.ticks())
        .left(x)
        .bottom(-5)
        .height(5)
      .anchor("bottom").add(pv.Label)
        .text(x.tickFormat);
    
    vis.add(pv.Rule)
        .bottom(0);
    
    vis.render();
    return vis
}