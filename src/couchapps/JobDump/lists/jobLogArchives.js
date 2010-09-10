function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  send("<html><body><pre>\n");
  send("Log Archive LFNs:\n");

  while (row = getRow()) {
    send("  " + row.value["retrycount"] + " -> " + row.value["lfn"] + "\n");
  }

  send("</pre></body></html>");
};
