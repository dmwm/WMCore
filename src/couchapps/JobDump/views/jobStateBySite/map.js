function(doc) {
    if (doc['type'] == 'job') {
        var site = null;
        //search from last state.
        //if job is retried in different site, it will only count the last site.
        //if intermediate site information is needed modify code (don't break)
        var currentTime = Math.round((new Date()).getTime() / 1000);
        //emit(doc['states'][0].location, 1)
        var i; //index
        for (i in doc['states']) {
            tempSite = doc['states'][i].location;
            if (tempSite != "Agent") {
                site = tempSite;
            }
        };
        if (site) {
            if (doc['states'][i].newstate != "executing" &&
                (currentTime - doc['states'][i].timestamp) < 3600) {
                var state = doc['states'][i].newstate;
                //if it is cleanout state propagate old state
                if (state == "cleanout") {
                    var oldstate = doc['states'][i].oldstate;
                    if (oldstate == "exhausted") {
                        state = "jobfailed";
                    }
                    else {
                        state = oldstate;
                    }
                } else if (state == "retrydone") {
                	state = "jobfailed";
                }
                emit([site, state], 1);
            }
        }
    }
};
