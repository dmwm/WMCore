#!/usr/bin/env python
"""
Test case for Rucio WMCore Service class
"""
from __future__ import print_function, division, absolute_import

import os
from pprint import pprint

from rucio.client import Client as testClient

from WMCore.Services.Rucio.Rucio import Rucio
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

DSET = "/SingleElectron/Run2017F-17Nov2017-v1/MINIAOD"
BLOCK = "/SingleElectron/Run2017F-17Nov2017-v1/MINIAOD#f924e248-e029-11e7-aa2a-02163e01b396"


class RucioTest(EmulatedUnitTestCase):
    """
    Unit tests for Rucio Service module
    """

    def __init__(self, methodName='runTest'):
        # TODO figure out what's going on with CRIC mock
        super(RucioTest, self).__init__(methodName=methodName, mockCRIC=False)

        self.acct = "wmagent_testing"

        # HACK: do not verify the SSL certificate because docker images
        # do not contain the CA certificate bundle
        # Relying on the config file in the jenkins infrastructure is a PITA
        # so let's make sure to pass all the necessary arguments
        self.creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                      "client_key": os.getenv("X509_USER_KEY", "Unknown")}

        self.defaultArgs = {"host": 'http://cms-rucio-dev.cern.ch',
                            "auth_host": 'https://cms-rucio-auth-dev.cern.ch',
                            "auth_type": "x509", "account": self.acct,
                            "ca_cert": False, "timeout": 30, "request_retries": 3,
                            "creds": self.creds}

    def setUp(self):
        """
        Setup for unit tests
        """
        super(RucioTest, self).setUp()

        self.myRucio = Rucio(self.acct,
                             hostUrl=self.defaultArgs['host'],
                             authUrl=self.defaultArgs['auth_host'],
                             configDict=self.defaultArgs)

        self.client = testClient(rucio_host=self.defaultArgs['host'],
                                 auth_host=self.defaultArgs['auth_host'],
                                 account=self.acct,
                                 ca_cert=self.defaultArgs['ca_cert'],
                                 auth_type=self.defaultArgs['auth_type'],
                                 creds=self.defaultArgs['creds'],
                                 timeout=self.defaultArgs['timeout'])

    def tearDown(self):
        """
        Nothing to be done for this case
        """
        pass

    def testConfig(self):
        """
        Test service attributes and the override mechanism
        """
        for key in self.defaultArgs:
            self.assertEqual(getattr(self.myRucio.cli, key), self.defaultArgs[key])
        self.assertTrue(getattr(self.myRucio.cli, "user_agent").startswith("wmcore-client/"))
        self.assertTrue(getattr(self.client, "user_agent").startswith("rucio-clients/"))

        newParams = {"host": 'http://cms-rucio-dev.cern.ch',
                     "auth_host": 'https://cms-rucio-auth-dev.cern.ch',
                     "auth_type": "x509", "account": self.acct,
                     "ca_cert": False, "timeout": 5, "phedexCompatible": False}
        newKeys = newParams.keys()
        newKeys.remove("phedexCompatible")

        rucio = Rucio(newParams['account'], hostUrl=newParams['host'],
                      authUrl=newParams['auth_host'], configDict=newParams)

        self.assertEqual(getattr(rucio, "phedexCompat"), False)
        for key in newKeys:
            self.assertEqual(getattr(rucio.cli, key), newParams[key])

    def testGetAccount(self):
        """
        Test whether we can fetch data about a specific rucio account
        """
        res = self.client.get_account(self.acct)
        res2 = self.myRucio.getAccount(self.acct)
        self.assertEqual(res['account'], self.acct)
        self.assertEqual(res['status'], "ACTIVE")
        self.assertEqual(res['account_type'], "USER")
        self.assertTrue({"status", "account", "account_type"}.issubset(set(res2.keys())))
        self.assertTrue({self.acct, "ACTIVE", "USER"}.issubset(set(res2.values())))

    # @attr('integration')
    def testWhoAmI(self):
        """
        Test user mapping information from the request headers
        """
        res = dict(self.client.whoami())
        res2 = dict(self.myRucio.whoAmI())
        self.assertTrue({"status", "account"}.issubset(set(res.keys())))
        self.assertTrue(set(res.keys()) == set(res2.keys()))

    def testPing(self):
        """
        Tests server ping
        """
        res = self.client.ping()
        res2 = self.myRucio.pingServer()
        self.assertTrue("version" in res)
        self.assertItemsEqual(res, res2)

    def testGetBlocksInContainer(self):
        """
        Test `getBlocksInContainer` method, the ability to retrieve blocks
        inside a container.
        """
        # test a CMS dataset that does not exist
        res = self.myRucio.getBlocksInContainer("Alan")
        self.assertEqual(res, [])

        # provide a CMS block instead of a dataset
        res = self.myRucio.getBlocksInContainer(BLOCK)
        self.assertEqual(res, [])

        # finally provide a real CMS dataset
        res = self.myRucio.getBlocksInContainer(DSET)
        self.assertTrue(len(res) >= len([BLOCK]))
        self.assertIn(BLOCK, res)

    def testGetReplicaInfoForBlocks(self):
        """
        Test `getReplicaInfoForBlocks` method, the ability to retrieve replica
        locations provided a dataset or block. Same output as PhEDEx.
        """
        res = self.myRucio.getReplicaInfoForBlocks(block=BLOCK)
        self.assertEqual(len(res['phedex']['block']), 1)
        block = res['phedex']['block'].pop()
        self.assertEqual(block['name'], BLOCK)
        replicas = [item['node'] for item in block['replica']]
        self.assertTrue(len(replicas) > 0)

        # same test, but providing a dataset as input (which has 4 blocks)
        res = self.myRucio.getReplicaInfoForBlocks(dataset=DSET)
        self.assertTrue(len(res['phedex']['block']) >= 1)  # at this very moment, there are 11 replicas
        blocks = [item['name'] for item in res['phedex']['block']]
        self.assertTrue(BLOCK in blocks)
        for item in res['phedex']['block']:
            self.assertTrue(len(item['replica']) > 0)

    def testGetReplicaInfoForBlocksRucio(self):
        """
        Test `getReplicaInfoForBlocks` method, however not using
        the output compatibility with PhEDEx
        """
        theseArgs = self.defaultArgs.copy()
        theseArgs['phedexCompatible'] = False
        myRucio = Rucio(self.acct,
                        hostUrl=theseArgs['host'],
                        authUrl=theseArgs['auth_host'],
                        configDict=theseArgs)

        res = myRucio.getReplicaInfoForBlocks(dataset=DSET)
        self.assertTrue(isinstance(res, list))
        self.assertTrue(len(res) >= 1)  # at this very moment, there are 11 replicas
        blocks = [item['name'] for item in res]
        self.assertTrue(BLOCK in blocks)
        for item in res:
            self.assertTrue(len(item['replica']) > 0)

    def testGetPFN(self):
        """
        Test `getPFN` method
        """
        self.assertRaises(NotImplementedError, self.myRucio.getPFN)
