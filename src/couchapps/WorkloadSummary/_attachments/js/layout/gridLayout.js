gridLayout = function(parentPanel, layoutConfig, data, metaData) {

    // don't set the size of outermost panel yet until know the number of containers
    var vis;
    if (!parentPanel) {
        vis = new pv.Panel()
    }else {
        vis = parentPanel.add(pv.Panel)
    };

   if (layoutConfig.label) {
        var label = vis.add(pv.Label);
        //set default label position
        label.top(10).left(100).text(metaData);
        
        for (prop in layoutConfig.label) {
            label[prop](layoutConfig.label[prop]);
        }
    }
    
    var maxWidth = 0;
    
    layoutConfig.width = 0;
    layoutConfig.height = 0;
    var cIndex = 1;
    var rIndex = 1;
    var numOfContainers = 0;

    var childLayoutPanel = vis;

    function moveToNextPosition (config) {
       if (cIndex < layoutConfig.numOfColumns) {
           config.left = config.left + config.width + config.wPadding * 2;
           cIndex += 1;
           layoutConfig.width += (config.width + config.wPadding * 2);
           layoutConfig.height = (config.height + config.hPadding * 2);
           if (maxWidth < layoutConfig.width) {maxWidth = layoutConfig.width};
       }
       else {
           cIndex = 1;
           layoutConfig.width = (config.width + config.wPadding * 2);
           if (maxWidth < layoutConfig.width) {maxWidth = layoutConfig.width};
           config.left = layoutConfig.wPadding;
           config.top = config.top + config.height + config.hPadding * 2;
           rIndex += 1;
           layoutConfig.height += (config.height + config.hPadding * 2);
       }
       return config;
    }
    var tempHeight  = 0;
    var config = layoutConfig.childConfig;
        config.left = layoutConfig.wPadding; 
        config.top = layoutConfig.hPadding; 

    for (var element in data) {
       if (!element) continue;
       layoutConfig.childConfig.func(vis, config, data[element], element);
       moveToNextPosition(config);
       numOfContainers += 1;
    };

    layoutConfig.width = maxWidth;
    
    if (layoutConfig.wPaddingAddition) {
        layoutConfig.width += layoutConfig.wPaddingAddition; 
    };
    
    if (layoutConfig.hPaddingAddition) {
        layoutConfig.height += layoutConfig.hPaddingAddition; 
    };
    
    vis.width(layoutConfig.width)
       .height(layoutConfig.height)
       .top(layoutConfig.top + layoutConfig.hPadding)
       .left(layoutConfig.left + layoutConfig.wPadding);
    return vis;
}
