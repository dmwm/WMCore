function(newDoc, oldDoc, userCtx) {
    // We only care if the user is someone with the correct permissions
    // there is no difference between creating a new doc or updating an old one

    var validation = require("lib/validate").init(newDoc, oldDoc, userCtx);

    // if the oldDoc is exists and try to update, something is wrong
    if(oldDoc) {
        throw {forbidden: "Summary is already exist"};
    }
    // Admins can do anything
    if (validation.isAdmin()) {
        return true;
    }

    // Either Developer or DataOps Operator/Manager required
    if (validation.hasGroupRole("dataops", "developer") ||
        validation.hasGroupRole("dataops", "production-operator") ||
        validation.hasGroupRole("dataops", "production-manager")) {
        return true;
    }

    // authentication failed
    log("Authentication failed: " + toJSON(userCtx));
    throw {forbidden: "User not validated for action"};
}