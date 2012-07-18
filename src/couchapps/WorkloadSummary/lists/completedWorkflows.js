/**
 * This list function gets the view output from outputDataBy<*>
 * and creates an html which contains the links to the
 * performance histogram and summary for each row in the output view
 */
function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  var response = "<html><head>\n";
  response += "<title> Summary of completed worklows </title>\n";
  response += "</head><body style=\"font-family: arial;\">\n";

  while (row = getRow()) {
    var key = row.key;
    var id = row.id;
    response += "<b>Run:</b> " + key[2] + "\t";
    response += "<b>Primary dataset:</b> " + key[0] + "\n<br>\n";
    response += "<div id=output style=\"margin: 0px 0px 0px 15px;\">\n";
    response += "Workflow Summary: <a href=\"../../_show/histogramByWorkflow/"+ id + "\">" + id + "</a><br>\n";
    response += "</div><br>\n";
  }

  response += "</body></html>";
  return response
};

