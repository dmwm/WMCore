"""
Unit tests for REST/Auth.py module
Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import logging
import unittest

# third party modules
import cherrypy

# WMCore modules
from WMCore.REST.Auth import user_info_from_headers


class RESTAuthTest(unittest.TestCase):
    "Unit test for RESTAuth module"

    def setUp(self):
        """
        setup necessary data for unit test usage
        """
        self.logger = logging.getLogger('rest_auth')

        # REST/Auth.py module calculates checksum based on provided hmac
        # for instnace for self.hmac we will expect self.chkSum
        # with all CMS headers

        # let's create set of different conditions
        self.user_groups = []
        self.hmacs = []
        self.chkSums = []

        # for group:users group:admin
        user_groups = 'group:users group:admin'
        hmac = '169d02b96265caf05894b526f99a22549dcd38ed'
        chkSum = 'd357b299194ec4bed4e4fc73fc9ceab10139c16f'
        self.user_groups.append(user_groups)
        self.hmacs.append(hmac)
        self.chkSums.append(chkSum)

        # let's reverse order of user groups
        user_groups = 'group:admin group:users'
        self.user_groups.append(user_groups)
        self.hmacs.append(hmac)
        self.chkSums.append(chkSum)

        # for group:admin group:users iam_group:test site:T1_XX_YYYY
        user_groups = 'group:admin group:users iam_group:test site:T1_XX_YYYY'
        hmac = '57ea0f58134aa079972da30a8fc2bf81853c949b'
        chkSum = 'd357b299194ec4bed4e4fc73fc9ceab1013'
        self.user_groups.append(user_groups)
        self.hmacs.append(hmac)
        self.chkSums.append(chkSum)

    def testRESTAuth(self):
        "test RESTAuth methods"
        for idx in range(len(self.user_groups)):
            user_groups = self.user_groups[idx]
            hmac = self.hmacs[idx]
            chkSum = self.chkSums[idx]

            # for testing purposes we need to setup cherrypy headers
            # cms-auth-status, cms-authn, cms-authz, cms-authn-hmac, cms-authn-name, cms-authn-dn
            cherrypy.request.headers = {
                'cms-auth-status': 'OK',
                'cms-authn': 'fake',
                'cms-authz-user': user_groups,
                'cms-authn-hmac': hmac,
                'cms-authn-name': 'test',
                'cms-authn-dn': 'dn'}

            authzKey = bytes(chkSum, 'UTF-8')
            user = user_info_from_headers(key=authzKey)
            self.logger.info(f"user_groups {user_groups}")
            self.logger.info(f"hmac {hmac}")
            self.logger.info(f"chkSum {chkSum}")
            self.logger.info(f"user {user}")


if __name__ == '__main__':
    unittest.main()
