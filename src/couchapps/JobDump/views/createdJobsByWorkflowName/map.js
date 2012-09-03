function(doc) {
    var stateList = new Array();
    for (var transitionIndex in doc['states']) {
        stateList.push(doc['states'][transitionIndex]);
    }

    lastTransition = stateList.pop();
    if (lastTransition.newstate == "created") {
        emit ([doc.workflow, doc._id], 1);
    }
}

