#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module which constructs the ReqMgr2 authorization setup,
defining which CRIC roles and groups can perform which
actions, based on the target request status.

It defines 3 different permissions, such as:
* admin: can perform any status transition and non-status write action
* ops: can perform many status transition and non-status write action,
    but not all of them (service-based actions are restricted).
* ppd: can perform only a few status transition and non-status write
   action. Service and Ops-based actions are forbidden

The 2 data structures required for this module are defined in the
ReqMgr2 service configuration file, a short explanation of them follows:
 * authzByStatus: a list with 3 dictionaries in it. Each dictionary
     defines a "permission" level and which target statuses are allowed.
     E.g.:
         [{"permission": "admin",
           "statuses": ["acquired", "running-open"]},
          {"permission": "ops",
           "statuses": ["assigned", "staging", "staged"]},
          {"permission": "ppd",
           "statuses": ["new", "aborted", "NO_STATUS"]}]
 * authzRolesGroups: a dictionary defining which CRIC roles/groups
     are member of a given permission level. E.g.:
         {"admin": {'role': ["role_1"], 'group': ["group_1"]},
          "ops": {'role': ["role_1", "role_2"], 'group': ["group_1", "group_2"]},
          "ppd": {'role': ["role_1", "role_2", "role_3"], 'group': ["group_1", "group_2", "group_3"]}}

NOTE that "NO_STATUS" defines who is allowed to perform a write action
not involving request status transition
"""
from copy import deepcopy

from WMCore.ReqMgr.DataStructs.RequestStatus import ALLOWED_ACTIONS_FOR_STATUS


class AuthzByStatus:
    """
    Class defining the authorization policy based on the
    target request status.

    This class depends on the ReqMgr2 authorization configuration.
    """

    def __init__(self, authzByStatus, authzRolesGroups):
        """
        Create a request status based data structure, with the role and
        groups passed from the service configuration file.
        :param authzByStatus: a list of dictionary items. Where each dict contains
            a permission group and a list of allowed statuses.
        :param authzRolesGroups: a nested dictionary with CRIC roles and groups
            permissions for each permissions group
        :return: None
        """
        self.noStatus = None
        self.authzByStatus = {}
        self.authzRolesGroups = authzRolesGroups

        # first, map status to required permission
        for item in authzByStatus:
            for status in item["statuses"]:
                if status == "NO_STATUS":
                    self.noStatus = item["permission"]
                else:
                    self.authzByStatus[status] = item["permission"]

        # now validate the authorization configuration
        self._validateAuthzStruct()

    def _validateAuthzStruct(self):
        """
        Runs a basic validation of the authorization data structure
        to catch possible mistakes made in the configuration file.
        :return: raises an exception if problems are found
        """
        if self.noStatus is None:
            msg = "Configuration for authorization by status does not define the required "
            msg += "roles and groups for a non-status change update. Please review it."
            raise RuntimeError(msg)

        expectedPermissions = set(["admin", "ops", "ppd"])
        if set(list(self.authzRolesGroups)) != expectedPermissions:
            msg = "Authorized roles and groups do not match the expected group permissions, "
            msg += f"they are: {expectedPermissions}. Please review it."
            raise RuntimeError(msg)

        if set(self.authzByStatus.values()) != expectedPermissions:
            msg = "Configuration for authorization by status does not match the expected "
            msg += f"group permissions, they are: {expectedPermissions}. Please review it."
            raise RuntimeError(msg)

        if len(list(ALLOWED_ACTIONS_FOR_STATUS.keys())) != len(self.authzByStatus.keys()):
            msg = "Configuration for authorization by status is missing workflow "
            msg += "status(es). Please review it and make sure all statuses are covered."
            raise RuntimeError(msg)

    def getRolesGroupsByStatus(self, requestArgs):
        """
        Given a dictionary with the request arguments being changed by
        the user, decide which roles/groups the user needs to have in
        order to complete the desired action.
        :param requestArgs: dictionary with request arguments posted by the user
        :return: dictionary with the roles and groups required for such action
        """
        targetStatus = requestArgs.get("RequestStatus", None)
        if targetStatus is None:
            return deepcopy(self.authzRolesGroups[self.noStatus])

        permissionGroup = self.authzByStatus[targetStatus]
        return deepcopy(self.authzRolesGroups[permissionGroup])
