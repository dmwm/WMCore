function(head, req) {
  var row;
  var output = {}

  while (row = getRow()) {
    var dataset = row.value["dataset"];
    var task = row.value["task"];

    if (!(dataset in output)){
      output[dataset] = {};
    }
    output[dataset][task] = null;
  }

  send(toJSON(output))
}
