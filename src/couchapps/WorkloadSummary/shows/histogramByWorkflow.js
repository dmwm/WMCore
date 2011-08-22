function(doc, req) {
    if (doc == null) {
       return {body: "Error: unknown workflow name"};
    }

    var Mustache = require("lib/mustache");
    var mainDoc = this;
    var requestInfo = {};
    // req.docId is workflow name
    requestInfo.workflow = doc._id;
    var data = {};
    var dataExist = false;
    for (var stat in doc.performance) {
        if (doc.performance[stat].histogram){
            data[stat] = doc.performance[stat].histogram;
            dataExist = true;
        };
    };
    if (dataExist) {
        //return JSON.stringify(data);
        requestInfo.data = JSON.stringify(data);
        var html = Mustache.to_html(mainDoc.templates.histogramByWorkflow, 
                                   requestInfo)
        return {body: html};
    } else {
        return {body: "No histogram data for " + req.docId}
    }
}