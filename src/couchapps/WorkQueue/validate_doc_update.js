function(newDoc, oldDoc, userCtx) {
    // Check permissions and filter out replication of _deleted docs

    if (newDoc._deleted === true && !oldDoc) {
      throw({forbidden: 'Do not create deleted docs'});
    }

    var validation = require("lib/validate").init(newDoc, oldDoc, userCtx);

    // Admins can do anything
    if (validation.isAdmin()) {
        return true;
    }

    // Either Developer or DataOps Operator/Manager required
    if (validation.hasGroupRole("dataops", "developer") ||
        validation.hasGroupRole("dataops", "production-operator") ||
        validation.hasGroupRole("dataops", "production-manager") ||
        validation.hasGroupRole("facops", "web-service")) {
        return true;
    }

    // authentication failed
    log("Authentication failed: " + toJSON(userCtx));
    throw {forbidden: "User not validated for action"};
}