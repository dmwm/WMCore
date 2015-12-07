"""
Define Authentiction roles and groups for the access permission for writing the the database
WMCore.REST.Auth authz_match
"""
ALL_ROLES = ['developer', 'admin', 'data-manager', 'production-operator', 'web-service']
ADMIN_ROLES = ['admin', 'web-service']
ADMIN_GROUP = ['reqmgr', 'facops']
#OTHER_GROUP = ['dataops']

DEFAULT_PERMISSION =  {'role': ALL_ROLES,
                      'group': []}

ADMIN_PERMISSION = {'role': ADMIN_ROLES, 'group': ADMIN_GROUP}

DEFAULT_STATUS_PERMISSION = {'role': ALL_ROLES,
                             'group': []}

CREATE_PERMISSION = ADMIN_PERMISSION

ASSIGN_PERMISSION = DEFAULT_STATUS_PERMISSION

APPROVE_PERMISSION = DEFAULT_STATUS_PERMISSION

STORE_RESULT_ASSIGN_PERMISSION = DEFAULT_PERMISSION

STORE_RESULT_APPROVE_PERMISSION = DEFAULT_PERMISSION

STORE_RESULT_CREATE_PERMISSION = CREATE_PERMISSION

def getPermissionByStatusType(requestType, requestStatus):
    if requestType == 'StoreResult':
        return {'assigned': STORE_RESULT_ASSIGN_PERMISSION,
                'assign-approved': STORE_RESULT_APPROVE_PERMISSION,
                'new': CREATE_PERMISSION
               }.get(requestStatus, DEFAULT_STATUS_PERMISSION)
    else:
        return {'assigned': ASSIGN_PERMISSION,
                'assign-approved': APPROVE_PERMISSION,
                'new': CREATE_PERMISSION
               }.get(requestStatus, DEFAULT_STATUS_PERMISSION)

def getWritePermission(request_args, config=None):
    requestType = request_args["RequestType"]
    requestStatus = request_args.get("RequestStatus", None)
    if requestStatus:
        return getPermissionByStatusType(requestType, requestStatus);
    else:
        return ADMIN_PERMISSION 
