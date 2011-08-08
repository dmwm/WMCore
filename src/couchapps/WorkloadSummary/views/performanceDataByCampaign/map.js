function(doc) {
    //The structure of histogram is
    // [{upperEdge: 2.47, average: 1.47, nEvent: 10, stdDev: 0, type: "standard",
    //   lowerEdge: 0.478}, .... ] 
    //if (doc.campaign) {
        var data = {};
        var emitable = false;
        for (var stat in doc.performance) {
            if (doc.performance[stat].histogram){
                data[stat] = doc.performance[stat].histogram;
                emitable = true;
                //emit([doc.campaign, doc._id], data);
            }
        }
        if (emitable) {
            emit([doc.campaign, doc._id], data);
        }
    //};
}