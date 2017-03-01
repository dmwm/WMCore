from __future__ import print_function, division

ALL_ROLES = ['developer', 'admin', 'data-manager', 'production-operator', 'web-service']
ALL_GROUP = ['reqmgr', 'facops', 'dataops']
ADMIN_ROLES = ['admin', 'web-service']
ADMIN_GROUP = ['reqmgr', 'facops']

ADMIN_PERMISSION = {'role': ADMIN_ROLES, 'group': ADMIN_GROUP}
DEFAULT_STATUS_PERMISSION = {'role': ALL_ROLES, 'group': ALL_GROUP}


PERMISSION_BY_REQUEST_TYPE = {
    "ADMIN" : ADMIN_PERMISSION,
    "NOSTATUS":  DEFAULT_STATUS_PERMISSION, # permmission for updating the values without updating status
    "DEFAULT_STATUS": DEFAULT_STATUS_PERMISSION, # permmission for updating the status change
    "DEFAULT_TYPE": {"new": DEFAULT_STATUS_PERMISSION,
                     "assignment-approved": DEFAULT_STATUS_PERMISSION,
                     "assigned": DEFAULT_STATUS_PERMISSION},
    "StoreResult" : {"new": DEFAULT_STATUS_PERMISSION,
                     "assignment-approved": DEFAULT_STATUS_PERMISSION,
                     "assigned": DEFAULT_STATUS_PERMISSION}
    }
