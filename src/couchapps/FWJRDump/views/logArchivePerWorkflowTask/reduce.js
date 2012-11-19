function (keys, values) {
  output = {};
    for (var index in values) {
      logArchive = values[index];
      workflowTask = keys[index];
      if (output[workflowTask] == null) {
        output = [logArchive];                              
      }
      else {
        output.push(logArchive);
      }
    }
    return output;
}
