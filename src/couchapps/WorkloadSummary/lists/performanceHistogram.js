function(head, req) {
    // this function is used for elementsDetailByWorkflowAndStatus
    var mainDoc = this
    provides("html", function() {
        var Mustache = require("lib/mustache");
        var requestInfo;
        if (req.query.key) {
            requestInfo = {campaign: req.query.key[0]};
        } else {
            requestInfo = {campaign: null};
        }
        return Mustache.to_html(mainDoc.templates.performanceData,
                                requestInfo, null, send);
    })
} // end function