function(head, req) {
    // this function is used for elementsDetailByWorkflowAndStatus
    var mainDoc = this;
    provides("html", function() {
        var Mustache = require("lib/mustache");
        var requestInfo;
        if (req.query.startkey) {
            requestInfo = {request: req.query.startkey[0]};
        } else {
            requestInfo = {request: ""};
        }
        return Mustache.to_html(mainDoc.templates.ElementSummaryByWorkflow,
                                requestInfo, mainDoc.templates.partials, send);
    });

    provides("json", function() {
        send("[");
        var row = getRow();
        if (row) {
            send(toJSON(row.value));
            while (row = getRow()){
                send(",");
                send(toJSON(row.value));
            }
        }// end rows
        send("]");
    });
}; // end function