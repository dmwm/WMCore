"""
Define Authentiction roles and groups for the access permission for writing the the database
WMCore.REST.Auth authz_match
"""
from __future__ import print_function, division

from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache

def getWritePermission(request_args):

    requestType = request_args["RequestType"]
    requestStatus = request_args.get("RequestStatus", None)

    permission_config = ReqMgrConfigDataCache.getConfig("PERMISSION_BY_REQUEST_TYPE")
    default_permission = permission_config["DEFAULT_STATUS"]
    if requestStatus is None:
        return permission_config["NOSTATUS"]
    elif requestType not in permission_config:
        return permission_config["DEFAULT_TYPE"].get(requestStatus, default_permission)
    else:
        return permission_config[requestType].get(requestStatus, default_permission)

