function(newDoc, oldDoc, userCtx) {
    // We only care if the user is someone with the correct permissions
    // there is no difference between creating a new doc or updating an old one

    var validation = require("lib/validate").init(newDoc, oldDoc, userCtx);

    // Gets whether the user is a global admin
    // name=null means requests coming from the local replicator, so we must allow
    // (the cms couch auth does not allow name=null, so it affects only internal requests)
    var isGlobalAdm = (userCtx.name === null)

    // Admins can do anything
    if (validation.isAdmin() || isGlobalAdm) {
        return true;
    }

    // Either Developer or DataOps Operator/Manager required
    if (validation.hasGroupRole("dataops", "developer") ||
        validation.hasGroupRole("dataops", "production-operator")) {
        return true;
    }

    // authentication failed
    log("Authentication failed: " + toJSON(userCtx));
    throw {forbidden: "User not validated for action"};
}
