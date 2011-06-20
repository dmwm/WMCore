function(doc, req) {
    resp = "<html><head>";
    resp += "<title>Ops Clipboard View</title>";
    resp += "<script src=\"/_utils/script/json2.js\"></script>\n";
    resp += "<script src=\"/_utils/script/jquery.js?1.3.1\"></script>\n";
    resp += "<script src=\"/_utils/script/jquery.couch.js?0.9.0\"></script>\n";
    resp += "<script src=\"../../vendor/opsclipboard/opsclipboard.js\"></script>\n";
    resp += "<script src=\"../../vendor/opsclipboard/opsstates.js\"></script>\n";
    resp += "</head><body>";
    resp += "<p> Document Show for " + doc._id + "</p>";
    resp += "<div id=\"clipboard\"></div>";
    resp += "</body>";
    resp += "<script type=\"text/javascript\">\n";
    resp += "  opsclipboard.setCouchDB();\n";
    resp += "  opsclipboard.clipboardview(\"" + doc._id + "\", document.getElementById(\"clipboard\"));"
    resp += "  opsclipboard.update();"
    resp += "</script>";
    resp += "</html>";
    
    return resp;   
}