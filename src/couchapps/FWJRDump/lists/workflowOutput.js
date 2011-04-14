function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  while (row = getRow()) {
    var numFiles = row.value["count"];
    var numEvents = row.value["events"];
    var numBytes = row.value["size"];
    var dataset = row.value["dataset"];

    if (numFiles == 1) {
      var filesString = numFiles + " file, ";
    } else {
      var filesString = numFiles + " files, ";
    }

    if (numEvents == 1) {
      var eventsString = numEvents + " event, ";
    } else {
      var eventsString = numEvents + " events, ";
    }

    if (numBytes == 1) {
      var bytesString = numBytes + " byte";
    } else if ((numBytes / 1000000000) > 1) {
      var bytesString = (numBytes / 1000000000.0).toFixed(2) + " GB";
    } else if ((numBytes / 1000000) > 1) {
      var bytesString = (numBytes / 1000000.0).toFixed(2) + " MB";
    } else if ((numBytes / 1000) > 1) {
      var bytesString = (numBytes / 1000.0).toFixed(2) + " KB";
    } else {
      var bytesString = numBytes + " bytes";
    }

    send("  " + dataset + " " + filesString + " " + eventsString + " " + bytesString + "<br>\n");
  }
};
