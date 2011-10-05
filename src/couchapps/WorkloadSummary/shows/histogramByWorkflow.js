function(doc, req) {
    function isEmpty(obj) {
        for (a in obj) {
            return false;
        };
        return true;
    };
    
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
    for (var task in doc.performance) {
        data[task] = {};
        for (var step in doc.performance[task]) {
            data[task][step] = {};
            for (var stat in doc.performance[task][step]) {
                if (doc.performance[task][step][stat].histogram && 
                    doc.performance[task][step][stat].histogram.length){
                    data[task][step][stat] = doc.performance[task][step][stat].histogram;
                    dataExist = true;
                };
            };
            if (isEmpty(data[task][step])) {delete data[task][step]};
        };
        if (isEmpty(data[task])) {delete data[task]};
    }
    var errors = {};
    for (var task in doc.errors) {
        errors[task] = {};
        for (var step in doc.errors[task]) {
            errors[task][step] = {};
            for (var code in doc.errors[task][step]) {
                dataExist = true;
            }
        }
    };
    
    requestInfo.errors  = {};
    if (!isEmpty(doc.errors)) {
        requestInfo.errors = JSON.stringify(doc.errors);
    };
    
    requestInfo.output = {};
    if (!isEmpty(doc.output)) {
        requestInfo.output = JSON.stringify(doc.output);
        dataExist = true;
    };

    provides("html", function() {
        if (dataExist) {
            //return JSON.stringify(data);
            requestInfo.data = JSON.stringify(data);
            var html = Mustache.to_html(mainDoc.templates.histogramByWorkflow, 
                                       requestInfo)
            return {body: html,
                    headers: { "Content-Type": "text/html"}};
        } else {
            return {body: "No histogram data for " + doc._id,
                    headers: { "Content-Type": "text/html"}};
        }
    });

    provides("json", function() {
        return {body : JSON.stringify(data),
            headers: {"Content-Type": "application/json"}};
    });
}