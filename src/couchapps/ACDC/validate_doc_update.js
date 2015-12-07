function(newDoc, oldDoc, userCtx) {
    // Determines the doc operation type
    var DOCOPS = {
        modif : 0,
        creat : 1,
        delet : 2
    };
    var docOp = oldDoc ? (newDoc._deleted === true ? DOCOPS.delet : DOCOPS.modif) : DOCOPS.creat;

    // Function to get the user list of site/groups for the given role
    var getRole = function(role) {
        var roles = userCtx.roles;
        for (i in roles) {
            if ( typeof (roles[i]) == "object" && roles[i][0] === role)
                return roles[i][1];
            // Request comes from backend auth handler
            if ( typeof (roles[i]) == "string" && roles[i] === role)
                return [];
            // Request comes from other handlers (i.e. host auth)
        }
        return null;
    }
    // Function to check if user has the role for a given group or site
    var matchesRole = function(role, grpsite) {
        var r = getRole(role);
        if (r != null)
            if (grpsite === "" || r.indexOf(grpsite) != -1)
                return true;
        return false;
    }
    // Gets whether the user is a global admin
    // name=null means requests coming from the local replicator, so we must allow
    // (the cms couch auth does not allow name=null, so it affects only internal
    // replication requests)
    var isGlobalAdm = (userCtx.name === null) ||
                      matchesRole("_admin", "") ||
                      matchesRole("-admin", "group:couchdb");

    //---------------------------------
    // Authorization rules for Myapp DB

    // The following rule aplies for all operation types
    var allowed = isGlobalAdm ||
                  matchesRole("admin", "group:reqmgr") ||
                  matchesRole("web-service", "group:facops") ||
                  matchesRole("production-operator", "group:dataops");

    // Throw if user not validated
    if (!allowed) {
        log(toJSON(userCtx));
        throw {
            forbidden : "User not authorized for action."
        };
    }
}
