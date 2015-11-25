function(head, req) {
    // this function is used for elementsDetailByWorkflowAndStatus
    var mainDoc = this;
    provides("html", function() {
        var Mustache = require("lib/mustache");
        var requestInfo;
        if (req.query.key) {
            requestInfo = {request: req.query.key};
        } else {
            requestInfo = {request: ""};
        }
        return Mustache.to_html(mainDoc.templates.WorkflowSummary,
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
} // end function