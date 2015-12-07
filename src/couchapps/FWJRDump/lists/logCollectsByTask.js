function(head, req) {
  var row;
  var output = {}

  while (row = getRow()) {
    var task = row.key;
    var pfn = row.value;

    if (!(task in output)){
      output[task] = [];
    }
    output[task].push(pfn);
  }

  send(toJSON(output))
}
