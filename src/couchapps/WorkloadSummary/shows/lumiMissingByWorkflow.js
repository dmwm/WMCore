function(doc, req) {

  // No document found, return a nice Four Oh Four
  if (doc === null){
    return {"code"   : 404,
            "body"   : "Workflow name doesn't exist.",
            "headers": { "Content-Type": "text/html"}};
  }

  // They may want just the lost lumi in an output dataset
  var searchKey = req.query.outputDataset;

  // Initialize the result
  var result = new Object;

  // Go through the errors
  for(var task in doc.errors){
    var resultKeys = [];
    // Check if we have output dataset - task mapping information
    for (var outputDataset in doc.output){
      var outputInfo = doc.output[outputDataset];
      if("tasks" in outputInfo){
        if(outputInfo.tasks.indexOf(task) != -1){
          resultKeys.push(outputDataset);
        }
      }
    }

    // If we only have task for the errors, then that will be the key
    if(resultKeys.length === 0){
      resultKeys.push(task);
    }
    // Now record the lumi lost from all the errors
    var lumiLost = new Object;
    for (var step in doc.errors[task]){
      for (var exitCode in doc.errors[task][step]){
        for (var run in doc.errors[task][step][exitCode]["runs"]){
          if (!(run in lumiLost)){
            lumiLost[run] = new Object;
          }
          for(var i = 0; i < doc.errors[task][step][exitCode]["runs"][run].length; i++){
            singleLumi = doc.errors[task][step][exitCode]["runs"][run][i]
            lumiLost[run][singleLumi] = true;
          }
        }
      }
    }
    // Add the lumi lost to the appropiate dataset or task
    // As always avoid duplicates by using Objects instead of Arrays
    for (var i = 0; i < resultKeys.length; i++){
      var individualKey = resultKeys[i];
      if (!(individualKey in result)){
        result[individualKey] = new Object;
      }
      for (var run in lumiLost){
        if (!(run in result[individualKey])){
          result[individualKey][run] = new Object;
        }
        for (var singleLumi in lumiLost[run]){
          result[individualKey][run][singleLumi] = true;
        }
      }
    }
  }

  // We finished gathering all the lumis, generate a user-friendly
  // structure like {OutputDataset : {run : [lumi,lumi]} }
  var jsonResult = new Object;
  for (var indexingKey in result){
    if (searchKey != null && searchKey != indexingKey) continue;
    jsonResult[indexingKey] = new Object;
    for (var run in result[indexingKey]){
      jsonResult[indexingKey][run] = [];
      var allLumis = []
      for (var singleLumi in result[indexingKey][run]){
        allLumis.push(parseInt(singleLumi));
      }
      allLumis.sort(function(a,b){return a-b});
      if (allLumis.length === 0) continue;
      var lastLumi = allLumis[0]
      var lumiBlock = [lastLumi]
      for (var i = 1; i < allLumis.length; i++){
        var currentLumi = allLumis[i]
        if (currentLumi !== (lastLumi + 1)){
          lumiBlock[1] = lastLumi
          jsonResult[indexingKey][run].push(lumiBlock)
          lumiBlock = [currentLumi]
        }
        lastLumi = currentLumi
      }
     lumiBlock[1] = lastLumi
     jsonResult[indexingKey][run].push(lumiBlock)
    }
  }

  send(JSON.stringify(jsonResult));
}
