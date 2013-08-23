"""
Define Authentiction roles and groups for the access permission for writing the the database
WMCore.REST.Auth authz_match
"""

DEFAULT_PERMISSION =  {'role': ['Developer', 'Admin', 'Data Manager', 'developer', 'admin', 'data-manager'],
                      'group': []}

ADMIN_PERMISSION = {'role': ['Admin', 'admin'], 'group': ['ReqMgr', 'reqmgr']}

DEFAULT_STATUS_PERMISSION = {'role': ['Developer', 'Admin', 'Data Manager', 'developer', 'admin', 'data-manager'],
                             'group': []}

CREATE_PERMISSION = {'role': ['Developer', 'Admin', 'Data Manager', 'developer', 'admin', 'data-manager'],
                              'group': ['ReqMgr', 'reqmgr']}

ASSIGN_PERMISSION = DEFAULT_STATUS_PERMISSION

APPROVE_PERMISSION = DEFAULT_STATUS_PERMISSION

STORE_RESULT_ASSIGN_PERMISSION = {'role': ['Developer', 'Admin', 'Data Manager', 'developer', 'admin', 'data-manager'],
                                  'group': []}

STORE_RESULT_APPROVE_PERMISSION = {'role': ['Developer', 'Admin', 'Data Manager', 'developer', 'admin', 'data-manager'],
                                  'group': []}

STORE_RESULT_CREATE_PERMISSION = CREATE_PERMISSION

def getPermissionByStatusType(requestType, requestStatus):
    if requestType == 'StoreResult':
        return {'assigned': STORE_RESULT_ASSIGN_PERMISSION,
                'approved': STORE_RESULT_APPROVE_PERMISSION,
                'new': CREATE_PERMISSION
               }.get(requestStatus, DEFAULT_STATUS_PERMISSION)
    else:
        return {'assigned': ASSIGN_PERMISSION,
                'approved': APPROVE_PERMISSION,
                'new': CREATE_PERMISSION
               }.get(requestStatus, DEFAULT_STATUS_PERMISSION)

def getWritePermission(request_args, config=None):
    requestType = request_args["RequestType"]
    requestStatus = request_args.get("RequestStatus", None)
    if requestStatus:
        return getPermissionByStatusType(requestType, requestStatus);
    else:
        return ADMIN_PERMISSION 
