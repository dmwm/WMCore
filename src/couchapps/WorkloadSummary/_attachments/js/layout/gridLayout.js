gridLayout = function(layoutConfig, container, data) {
    if (!layoutConfig) {
        //set default layout
        layoutConfig = {};
        layoutConfig.top = 0;
        layoutConfig.left = 0;
        layoutConfig.width = 250;
        layoutConfig.height = 200;
        layoutConfig.wPadding = 50;
        layoutConfig.hPadding = 20; 
        layoutConfig.numOfColumns = 5;
     };
    
    // don't set the size of outermost panel yet until know the numbe of containers
    var vis = new pv.Panel();

    var cIndex = 0;
    var rIndex = 0;
    var numOfContainers = 0
    for (var element in data) {
       var config = {};
       config.width = layoutConfig.width; 
       config.height = layoutConfig.height; 
       if (cIndex < layoutConfig.numOfColumns) {
           config.left = layoutConfig.left + (config.width + layoutConfig.wPadding * 2) * cIndex;
           cIndex += 1;
           config.top = layoutConfig.top;
       } else {
           cIndex = 0;
           config.left = layoutConfig.left
           config.top = layoutConfig.top + (config.height + layoutConfig.hPadding * 2) * rIndex;
           rIndex += 1;
       }
       
       container(vis, config, element, data);
       numOfContainers += 1;
    };
    
    //calculate the size of the outermost container
    //If the numOfColums is bigger than numOfContainer just make it big enough
    //TODO if there is the problem resize it  
    var panelWidth = (layoutConfig.width + 2*layoutConfig.wPadding) * layoutConfig.numOfColumns,
        panelHeight = (layoutConfig.height + 2*layoutConfig.hPadding) * (Math.floor(numOfContainers/layoutConfig.numOfColumns) +1);
    vis.width(panelWidth)
       .height(panelHeight)
       .top(layoutConfig.top)
       .left(layoutConfig.left);
        
    return vis;
}
