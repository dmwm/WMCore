function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  send("Output Files:\n");

  while (row = getRow()) {
    send("  " + row.value["location"] + " -> " + row.value["lfn"] + "\n");
  }
};
