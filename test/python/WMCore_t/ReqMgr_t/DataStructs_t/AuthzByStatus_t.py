#!/usr/bin/env python
"""
Unit tests for the AuthzByStatus module
"""
import unittest

from WMCore.ReqMgr.DataStructs.AuthzByStatus import AuthzByStatus

ADMIN = {'role': ["admin_role"], 'group': ["admin_group"]}
OPS = {'role': ["admin_role", "ops_role"], 'group': ["admin_group", "ops_group"]}
PPD = {'role': ["admin_role", "ops_role", "ppd_role"],
       'group': ["admin_group", "ops_group", "ppd_group"]}


class AuthzByStatusTests(unittest.TestCase):
    """Some basic tests for AuthzByStatus"""

    def setUp(self):
        self.authorized_roles = {"admin": ADMIN,
                                 "ops": OPS,
                                 "ppd": PPD}
        self.authz_by_status = [{"permission": "admin",
                                 "statuses": ["acquired", "running-open", "running-closed", "completed",
                                              "aborted-completed", "failed", "rejected-archived",
                                              "aborted-archived", "normal-archived"]},
                                {"permission": "ops",
                                 "statuses": ["assigned", "staging", "staged", "force-complete",
                                              "closed-out", "announced"]},
                                {"permission": "ppd",
                                 "statuses": ["new", "assignment-approved", "rejected", "aborted", "NO_STATUS"]}]

        # self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        pass

    def testNoStatus(self):
        """Test case where NO_STATUS is not provided"""
        with self.assertRaises(RuntimeError):
            AuthzByStatus([], {})

    def testBadPermissions(self):
        """Test case for lack of group permissions"""
        with self.assertRaises(RuntimeError):
            AuthzByStatus([], {"admin": "a", "ops": "o"})

    def testBadStatusValue(self):
        """Test case for bad statuses-based permissions"""
        with self.assertRaises(RuntimeError):
            AuthzByStatus([{"permission": "ALAN", "statuses": ["new", "acquired"]}],
                          {"admin": "a", "ops": "o", "ppd": "e", "ALAN": "aaa"})

    def testBadStatusName(self):
        """Test case for an unexpected status name"""
        with self.assertRaises(RuntimeError):
            AuthzByStatus([{"permission": "admin", "statuses": ["new", "assigned"]},
                           {"permission": "ops", "statuses": ["staging", "staged"]},
                           {"permission": "ppd", "statuses": ["acquired", "Alan"]}],
                          {"admin": "a", "ops": "o", "ppd": "e"})

    def testCorrectConfig(self):
        """Test correct configuration for the authorization setup"""
        authzPerm = AuthzByStatus(self.authz_by_status, self.authorized_roles)

        # test no status permission
        resp = authzPerm.getRolesGroupsByStatus({})
        self.assertCountEqual(resp, PPD)

        # test admin permissions (and one allowed by Ops and PPD)
        for status in {"new", "failed", "normal-archived", "staged", "aborted"}:
            resp = authzPerm.getRolesGroupsByStatus({"RequestStatus": status})
            self.assertCountEqual(resp, ADMIN)

        # test Ops permissions (and one allowed by PPD)
        for status in {"assigned", "staged", "announced", "aborted"}:
            resp = authzPerm.getRolesGroupsByStatus({"RequestStatus": status})
            self.assertCountEqual(resp, OPS)

        # test PPD permissions
        for status in {"new", "assignment-approved", "rejected", "aborted"}:
            resp = authzPerm.getRolesGroupsByStatus({"RequestStatus": status})
            self.assertCountEqual(resp, OPS)
