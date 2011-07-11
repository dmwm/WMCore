function(head, req) {
  provides('json', function() {
    var row;
    start({"headers": {"Content-Type": "text/html"}});
    var lumiList = new Array();

    while (row = getRow()) {
      var runs = row.value["runs"];

      for (run in runs) {
        if (run in lumiList) {
          for (lumi in runs[run]) {
            lumiList[run].push(runs[run][lumi]);
          }
        } else {
          lumiList[run] = runs[run];
        }
      }
    }

    // Compact the list
    var compactList = new Object();
    for (run in lumiList) {
      compactList[run] = [];
      lumiList[run].sort(function(a,b){return a - b}) // Sort lumis in run numerically
      var begLumi = parseInt(lumiList[run][0]);
      var endLumi = parseInt(lumiList[run][0]);

      for (lumiNum in lumiList[run]) {
        lumi = parseInt(lumiList[run][lumiNum]);
        if (lumi == endLumi) { // Duplicate or first lumi in	list
          continue;
        } else if (lumi == endLumi+1) { // Just next in the list, add it
          endLumi = lumi;
        } else { // New range
          compactList[run].push([begLumi, endLumi]);
          begLumi = lumi;
          endLumi = lumi;
        }
      }
      compactList[run].push([begLumi, endLumi]); // Push out the remainder
    }

    send(JSON.stringify(compactList));
  });
};
