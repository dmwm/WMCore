function(doc, req) 
{
	resp  = "<html><head>";
    resp += "<meta http-equiv=\"Cache-Control\" content=\"no-cache, max-age=0, no-store, must-revalidate\">";
	resp += "<meta http-equiv=\"Pragma\" content=\"no-cache\">";
    resp += "<title>OpsClipboard request details</title>";
    resp += "<script src=\"../../vendor/couchapp/json2.js\"></script>\n";
    resp += "<script src=\"../../vendor/couchapp/jquery.js?1.3.1\"></script>\n";
    resp += "<script src=\"../../vendor/couchapp/jquery.couch.js?0.9.0\"></script>\n";
    resp += "<script src=\"../../vendor/opsclipboard/status.js\"></script>\n";
    resp += "<script src=\"../../vendor/opsclipboard/requestshow.js\"></script>\n";
    resp += "<script src=\"../../vendor/opsclipboard/utils.js\"></script>\n";
    resp += "</head><body>\n";
    resp += "<h2><p>Request \"" + doc.request.request_id + "\"</p></h2>\n";
    resp += "<div id=\"requestshowpaneldivid\"></div>\n";
    resp += "</body>\n";
    resp += "<script type=\"text/javascript\">\n";
    resp += "  requestShow.setUp();\n";
    resp += "  requestShow.requestShow(\"" + doc._id + "\", \"requestshowpaneldivid\");\n"
    resp += "  requestShow.requestShowUpdate();\n"
    resp += "</script>\n";
    resp += "</html>\n";
    return resp;   
}