// Idea from https://github.com/jchris/couchapp.org

exports.init = function(newDoc, oldDoc, userCtx) {
    var v = {};

    // Is this an admin
    v.isAdmin = function() {
        //allow local replication
        if (userCtx.name === null) return true;
        return userCtx.roles.indexOf('_admin') !== -1;
    };

    // Does person have the required role for the required group
    v.hasGroupRole = function(group, role) {
        for(var i = 0, l = userCtx.roles.length; i < l; i++) {
            var user_role = userCtx.roles[i][0];
            if (user_role === role) {
                var user_groups = userCtx.roles[i][1];
                if (user_groups.indexOf('group:' + group) !== -1) {
                    return true;
                }
            }
        }
        return false;
    };


    return v;
};