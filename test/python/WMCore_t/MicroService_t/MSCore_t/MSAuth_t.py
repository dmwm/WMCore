"""
Unit tests for MSAuth.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import os
import json
import unittest

# third party modules
import cherrypy

# WMCore modules
from WMCore.MicroService.MSCore.MSAuth import readAuthzRules, MSAuth


class MSAuthTest(unittest.TestCase):
    "Unit test for MSAuth module"

    def setUp(self):
        """
        setup necessary data for unit test usage
        """
        self.rules = [
            {"role": "admin", "group": "reqmgr", "service": "ms-pileup", "action": "create", "method": ["POST"]},
            {"role": "user", "group": "admin", "service": "ms-pileup", "action": "create", "method": ["POST"]}]
        # REST/Auth.py module calculates checksum based on provided hmac
        # for instnace for self.hmac we will expect self.chkSum
        # with all CMS headers we setup below
        # d357b299194ec4bed4e4fc73fc9ceab10139c16f vs. 4311773080ea12f4ea31a096da49de105083bb9e
        self.hmac = 'd357b299194ec4bed4e4fc73fc9ceab10139c16f'
        self.chkSum = '4311773080ea12f4ea31a096da49de105083bb9e'
        self.fname = '/tmp/ms-authz.json'

        # for testing purposes we need to setup cherrypy headers
        # cms-auth-status, cms-authn, cms-authz, cms-authn-hmac, cms-authn-name, cms-authn-dn
        cherrypy.request.headers = {
            'cms-auth-status': 'OK',
            'cms-authn': 'fake',
            'cms-authz-user': 'group:users group:admin',
            'cms-authn-hmac': self.hmac,
            'cms-authn-name': 'test',
            'cms-authn-dn': 'dn'}

    def tearDown(self):
        """Clean-up method for unit test"""
        if os.path.exists(self.fname):
            os.remove(self.fname)

    def testReadAuthzRules(self):
        "test MSAuth methods"
        rules = readAuthzRules(self.rules)
        self.assertCountEqual(self.rules, rules)

        with open(self.fname, 'w', encoding="utf-8") as ostream:
            ostream.write(json.dumps(self.rules))
        rules = readAuthzRules(self.fname)
        self.assertCountEqual(self.rules, rules)

    def testMSAuth(self):
        "test MSAuth methods"
        msConfig = {'authz_rules': self.rules, 'authz_key': bytes(self.chkSum, 'UTF-8')}
        mgr = MSAuth(msConfig)
        mgr.authorizeApiAccess('ms-pileup', 'create')

        # now let's try to check if we are not authorized
        with self.assertRaises(cherrypy.HTTPError):
            mgr.authorizeApiAccess('ms-pileup', 'wrong-method')
        with self.assertRaises(cherrypy.HTTPError):
            mgr.authorizeApiAccess('wrong-service', 'create')


if __name__ == '__main__':
    unittest.main()
