#!/usr/bin/env python
"""
Test case for Rucio WMCore Service class
"""
from __future__ import print_function, division, absolute_import

import os
from builtins import range

from future.utils import viewitems
from nose.plugins.attrib import attr
from rucio.client import Client as testClient

from Utils.PythonVersion import PY3
from WMCore.Services.Rucio import Rucio
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

# production/testbed container with 2 blocks
DSET = "/ZeroBias/Run2016B-UL16_ver2_forHarvestOnly-v1/DQMIO"
BLOCK = "/ZeroBias/Run2016B-UL16_ver2_forHarvestOnly-v1/DQMIO#0d25a8ce-ea25-422c-bf86-a37174e7f6a3"
FILE = "/store/data/Run2016B/ZeroBias/DQMIO/UL16_ver2_forHarvestOnly-v1/230000/0C743F08-12F0-D44D-BC39-A740F1F6D623.root"
# production/testbed pileup container with hundreds of blocks (only a few in testbed though)
PUDSET = "/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX"
PUBLOCK = "/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX#004d5f64-f280-4bad-b88b-4b6e52ea040d"
# production container with 4 blocks
DSET2 = "/Mustar_MuG_L10000_M-3750_TuneCP2_13TeV-pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_102X_mc2017_realistic_v7-v1/NANOAODSIM"
BLOCK2 = "/Mustar_MuG_L10000_M-3750_TuneCP2_13TeV-pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_102X_mc2017_realistic_v7-v1/NANOAODSIM#50aecec1-d5a8-4756-9ee4-f93995c5b524"


# integration containers with with 2 blocks
# DSET3 = "/NoBPTX/Integ_Test-ReReco_LumiMask_HG1812_Validation_TEST_Alan_v13-v11/AOD"
# DSET4 = "/NoBPTX/Integ_Test-ReReco_LumiMask_HG1812_Validation_TEST_Alan_v13-v11/DQMIO"


class RucioTest(EmulatedUnitTestCase):
    """
    Unit tests for Rucio Service module
    """

    def __init__(self, methodName='runTest'):
        # TODO figure out what's going on with CRIC mock
        super(RucioTest, self).__init__(methodName=methodName, mockCRIC=False)

        self.acct = "wma_test"
        self.acct_transf = "wmcore_transferor"

        # HACK: do not verify the SSL certificate because docker images
        # do not contain the CA certificate bundle
        # Relying on the config file in the jenkins infrastructure is a PITA
        # so let's make sure to pass all the necessary arguments
        self.creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                      "client_key": os.getenv("X509_USER_KEY", "Unknown")}

        self.defaultArgs = {"host": 'http://cms-rucio-int.cern.ch',
                            "auth_host": 'https://cms-rucio-auth-int.cern.ch',
                            "auth_type": "x509", "account": self.acct,
                            "ca_cert": False, "timeout": 30, "request_retries": 3,
                            "creds": self.creds}

    def setUp(self):
        """
        Setup for unit tests
        """
        super(RucioTest, self).setUp()

        self.myRucio = Rucio.Rucio(self.acct,
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
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        """
        Nothing to be done for this case
        """
        pass

    def createProductionRucio(self, account=None):
        """Create an object pointing to the Rucio Production instance"""
        # NOTE: do your best to avoid testing against production!!!
        newParams = {"auth_type": "x509", "ca_cert": False, "timeout": 50}
        prodRucio = Rucio.Rucio(account,
                                hostUrl='http://cms-rucio.cern.ch',
                                authUrl='https://cms-rucio-auth.cern.ch',
                                configDict=newParams)
        return prodRucio

    def testConfig(self):
        """
        Test service attributes and the override mechanism
        """
        for key in self.defaultArgs:
            self.assertEqual(getattr(self.myRucio.cli, key), self.defaultArgs[key])
        self.assertTrue(getattr(self.myRucio.cli, "user_agent").startswith("wmcore-client/"))
        self.assertTrue(getattr(self.client, "user_agent").startswith("rucio-clients/"))

        newParams = {"host": 'http://cms-rucio-int.cern.ch',
                     "auth_host": 'https://cms-rucio-auth-int.cern.ch',
                     "auth_type": "x509", "account": self.acct,
                     "ca_cert": False, "timeout": 5}
        newKeys = list(newParams)

        rucio = Rucio.Rucio(newParams['account'], hostUrl=newParams['host'],
                            authUrl=newParams['auth_host'], configDict=newParams)

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

    def testGetAccountUsage(self):
        """
        Test whether we can fetch the data usage for a given rucio account
        """
        # test against a specific RSE
        res = list(self.client.get_local_account_usage(self.acct, rse="T1_US_FNAL_Disk"))
        res2 = self.myRucio.getAccountUsage(self.acct, rse="T1_US_FNAL_Disk")
        self.assertEqual(res[0]["rse"], "T1_US_FNAL_Disk")
        self.assertTrue(res[0]["files"] >= 1)
        self.assertTrue(res[0]["bytes"] >= 1000)
        self.assertTrue("bytes_remaining" in res[0])
        self.assertTrue(res[0]["bytes_limit"] >= 0)
        self.assertEqual(res, res2)

        # test against all RSEs
        res = list(self.client.get_local_account_usage(self.acct))
        res2 = self.myRucio.getAccountUsage(self.acct)
        self.assertTrue(len(res) > 1)
        self.assertTrue(len(res2) > 1)
        # I have manually created a rule for this account, so it will be there...
        self.assertEqual(res, res2)

        # now test against an account that either does not exist or that we cannot access
        res = self.myRucio.getAccountUsage("admin")
        self.assertIsNone(res)

    def testGetAccountLimits(self):
        """
        Test whether we can fetch the data quota for a given rucio account
        """
        res = self.client.get_local_account_limits(self.acct_transf)
        res2 = self.myRucio.getAccountLimits(self.acct_transf)
        # {'T2_CH_CERN': 10000000000000, 'T2_US_Purdue': 5000000000000, 'T1_US_FNAL_Disk': 10000000000000}
        self.assertTrue(len(res) >= 3)
        self.assertTrue(len(res2) >= 3)
        for rse in ('T1_US_FNAL_Disk', 'T2_CH_CERN', 'T2_US_Purdue'):
            self.assertEqual(res[rse], res2[rse])

        # test an account that we have no access to
        res = self.myRucio.getAccountLimits("wma_prod")
        self.assertEqual(res, {})

        # finally, test an account that does not exist
        res = self.myRucio.getAccountLimits("any_random_account")
        self.assertEqual(res, {})

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
        with self.assertRaises(Rucio.WMRucioDIDNotFoundException):
            self.myRucio.getBlocksInContainer("Alan")

        # provide a CMS block instead of a dataset
        res = self.myRucio.getBlocksInContainer(BLOCK)
        self.assertEqual(res, [])

        # finally provide a real CMS dataset
        res = self.myRucio.getBlocksInContainer(DSET)
        self.assertTrue(len(res) >= len([BLOCK]))
        self.assertIn(BLOCK, res)

    def testGetReplicaInfoForBlocks(self):
        """
        Test `getReplicaInfoForBlocks` method.
        """
        res = self.myRucio.getReplicaInfoForBlocks(dataset=DSET)
        self.assertTrue(isinstance(res, list))
        self.assertTrue(len(res) >= 1)  # at this very moment, there are 11 replicas
        blocks = [item['name'] for item in res]
        self.assertTrue(BLOCK in blocks)
        for item in res:
            self.assertTrue(len(item['replica']) > 0)

        # Setting  deep=True should yield the same results
        res = self.myRucio.getReplicaInfoForBlocks(dataset=DSET, deep=True)
        self.assertTrue(isinstance(res, list))
        self.assertTrue(len(res) >= 1)  # Again, there are 11 replicas
        blocks = [item['name'] for item in res]
        self.assertTrue(BLOCK in blocks)
        for item in res:
            self.assertTrue(len(item['replica']) > 0)

    def testGetPFN(self):
        """
        Test `getPFN` method
        """
        ### New endpoint for Rucio Int
        cernTestbedPrefix = 'gsiftp://eoscmsftp.cern.ch:2811/eos/cms/store/test/rucio/int'

        testLfn = "/store"
        resp = self.myRucio.getPFN(site="T2_CH_CERN", lfns=testLfn)
        # self.assertEqual(resp[testLfn], cernTestDefaultPrefix + testLfn)
        self.assertEqual(resp[testLfn], cernTestbedPrefix + testLfn)

        # we do not rely on the following check with lfns=""
        # but since it currently works, it will be nice that it keeps working when
        # site TFCs will be replaced by prefixes and this may the only. Beware that currently
        # it only works for a subset of sites, as it relies on the specifics of their TFC.
        resp = self.myRucio.getPFN(site="T2_CH_CERN", lfns="")
        self.assertEqual(resp, {'': cernTestbedPrefix + "/"})

        # possible additional tests (from Stefano)
        lfn1 = '/store/afile'
        lfn2 = '/store/mc/afile'
        resp = self.myRucio.getPFN(site="T2_CH_CERN", lfns=lfn1)
        # self.assertEqual(resp[lfn1], cernTestDefaultPrefix + lfn1)
        self.assertEqual(resp[lfn1], cernTestbedPrefix + lfn1)

        # test with a list of LFN's
        resp = self.myRucio.getPFN(site="T2_CH_CERN", lfns=[lfn1, lfn2])
        self.assertEqual(len(resp), 2)
        # self.assertEqual(resp[lfn1], cernTestDefaultPrefix + lfn1)
        self.assertEqual(resp[lfn1], cernTestbedPrefix + lfn1)
        # self.assertEqual(resp[lfn2], cernTestDefaultPrefix + lfn2)
        self.assertEqual(resp[lfn2], cernTestbedPrefix + lfn2)

        # test different protocols
        resp = self.myRucio.getPFN(site="T2_US_Nebraska", lfns=lfn1, protocol='gsiftp')
        self.assertEqual(resp[lfn1],
                         "gsiftp://red-gridftp.unl.edu:2811/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms/store/test/rucio/int" + lfn1)
        # resp = self.myRucio.getPFN(site="T2_US_Nebraska", lfns=lfn1, protocol='xrootd')
        # self.assertEqual(resp[lfn1], "davs://xrootd-local.unl.edu:1094/store/test/rucio/int" + lfn1)

    @attr('integration')
    def testProdGetPFN(self):
        """
        Test `getPFN` method using the production server, hence set
        as an integration test not to be executed by jenkins
        """
        prodRucio = self.createProductionRucio("wmcore_transferor")
        # simplest call (from Alan)
        cernTestDefaultPrefix = "gsiftp://eoscmsftp.cern.ch:2811/eos/cms"
        testLfn = "/store"
        resp = prodRucio.getPFN(site="T2_CH_CERN", lfns=testLfn)
        self.assertEqual(resp[testLfn], cernTestDefaultPrefix + testLfn)

        # see comment in testGetPFN() function above about testing with lfns=""
        resp = prodRucio.getPFN(site="T2_CH_CERN", lfns="")
        self.assertItemsEqual(resp, {u'': u'gsiftp://eoscmsftp.cern.ch:2811/'})

        # possible additional tests (from Stefano)
        lfn1 = '/store/afile'
        lfn2 = '/store/mc/afile'
        resp = prodRucio.getPFN(site="T2_CH_CERN", lfns=lfn1)
        self.assertEqual(resp[lfn1], cernTestDefaultPrefix + lfn1)

        # test with a list of LFN's
        resp = prodRucio.getPFN(site="T2_CH_CERN", lfns=[lfn1, lfn2])
        self.assertEqual(len(resp), 2)
        self.assertEqual(resp[lfn1], cernTestDefaultPrefix + lfn1)
        self.assertEqual(resp[lfn2], cernTestDefaultPrefix + lfn2)

        # test different protocols
        resp = prodRucio.getPFN(site="T2_US_Nebraska", lfns=lfn1, protocol='gsiftp')
        self.assertEqual(resp[lfn1],
                         "gsiftp://red-gridftp.unl.edu:2811/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms" + lfn1)
        resp = prodRucio.getPFN(site="T2_US_Nebraska", lfns=lfn1, protocol='davs')
        self.assertEqual(resp[lfn1], "davs://xrootd-local.unl.edu:1094" + lfn1)

    def testListContent(self):
        """
        Test `listContent` method, to list content of a given DID
        """
        # listing blocks for a dataset
        res = self.myRucio.listContent(DSET)
        self.assertTrue(len(res) >= 2)
        self.assertEqual(res[0]["type"], "DATASET")

        # listing files for a block
        res = self.myRucio.listContent(BLOCK)
        self.assertTrue(len(res) > 5)
        self.assertEqual(res[0]["type"], "FILE")

        res = self.myRucio.listContent("/Primary/ProcStr-v1/tier")
        self.assertItemsEqual(res, [])

    def testListDataRules(self):
        """
        Test `listDataRules` method
        """
        res = self.myRucio.listDataRules(DSET)
        self.assertItemsEqual(res, [])

    @attr('integration')
    def testListDataRules2(self):
        """
        Test `listDataRules` method with data from production
        """
        prodRucio = self.createProductionRucio("wmcore_transferor")

        resp = prodRucio.listDataRules(name=DSET2, account="transfer_ops")
        self.assertEqual(len(resp), 5)
        accts = set([rule['account'] for rule in resp])
        self.assertItemsEqual(accts, ['transfer_ops'])

        resp = prodRucio.listDataRules(name=DSET2, account="wmcore_transferor")
        self.assertItemsEqual(resp, [])

    def testGetRule(self):
        """
        Test `getRule` method
        """
        # Badly formatted rule id, raises/catches a general exception
        res = self.myRucio.getRule("blah")
        self.assertItemsEqual(res, {})

        # Properly formatted rule, but inexistent id
        res = self.myRucio.getRule("1d6ea1d916d5492e81b1bb30ed4aebc0")
        self.assertItemsEqual(res, {})

        # Properly formatted rule, rule manually created
        res = self.myRucio.getRule("eb4448a33f8f4c3e8d162be42e7a2eb1")
        self.assertTrue(res)

    def testListParentDIDs(self):
        """
        Test `listParentDIDs` method, which lists the parent DIDs
        """
        # given a file, list its blocks (for now, single block)
        res = self.myRucio.listParentDIDs(FILE)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['scope'], "cms")
        self.assertEqual(res[0]['type'], "DATASET")
        self.assertEqual(res[0]['name'], BLOCK)

        # given a block, list its containers (for now, single container)
        res = self.myRucio.listParentDIDs(BLOCK)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['scope'], "cms")
        self.assertEqual(res[0]['type'], "CONTAINER")
        self.assertEqual(res[0]['name'], DSET)

        # given a container, returns nothing because containers have no parents
        res = self.myRucio.listParentDIDs(DSET)
        self.assertEqual(res, [])

    def testEvaluateRSEExpression(self):
        """
        Test the `evaluateRSEExpression` method
        """
        for i in range(2):
            res = self.myRucio.evaluateRSEExpression("tier=1", useCache=False)
            self.assertTrue(len(res) > 1)
            self.assertTrue("T1_US_FNAL_Disk" in res)
        self.myRucio.evaluateRSEExpression("tier=1", useCache=True)

    def testPickRSE(self):
        """
        Test the `pickRSE` method
        """
        print(self.myRucio.cli.list_rse_attributes("T1_US_FNAL_Disk"))
        resp = self.myRucio.pickRSE(rseExpression="ddm_quota>0", rseAttribute="ddm_quota")
        self.assertTrue(len(resp) == 2)
        self.assertTrue(resp[1] is True or resp[1] is False)

    def testRequiresApproval(self):
        """
        Test the `pickRSE` method
        """
        # NOTE: these are sort of test RSEs in Rucio Int
        resp = self.myRucio.requiresApproval("T1_US_FNAL_Tape_Input")
        self.assertTrue(resp)
        resp = self.myRucio.requiresApproval("T1_IT_CNAF_Tape_Input")
        self.assertFalse(resp)

    @attr('integration')  # jenkins cannot access this rucio account
    def testGetPileupLockedAndAvailable(self):
        """
        Test `getPileupLockedAndAvailable` method
        """
        # as much as I dislike it, we need to use the production instance...
        prodRucio = self.createProductionRucio("wmcore_transferor")
        resp = prodRucio.getPileupLockedAndAvailable(PUDSET, "transfer_ops")
        # this dataset contains 10 blocks
        self.assertTrue(len(resp) > 100)
        self.assertTrue(PUBLOCK in resp)
        # with more than 10 block replicas in the grid
        for block, rses in viewitems(resp):
            self.assertTrue(len(rses) >= 2)

    @attr('integration')  # jenkins cannot access this rucio account
    def testGetDataLockedAndAvailable(self):
        """
        Test `getDataLockedAndAvailable` method
        """
        # as much as I dislike it, we need to use the production instance...
        prodRucio = self.createProductionRucio("wmcore_transferor")

        # This is a very heavy check, with ~25k blocks
        # PUDSET = "/Neutrino_E-10_gun/RunIISpring15PrePremix-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v2-v2/GEN-SIM-DIGI-RAW"
        expectedRses = ['T2_CH_CERN', 'T1_US_FNAL_Disk', 'T1_UK_RAL_Disk', 'T1_UK_RAL_Tape']
        resp = prodRucio.getDataLockedAndAvailable(name=PUDSET)
        self.assertItemsEqual(resp, expectedRses)

        # matches rules for this container and transfer_ops account, returns an union of the blocks RSEs
        resp = prodRucio.getDataLockedAndAvailable(name=DSET2, account="transfer_ops")
        self.assertItemsEqual(resp, ['T2_CH_CSCS', 'T1_FR_CCIN2P3_Disk', 'T1_IT_CNAF_Disk', 'T2_US_Purdue',
                                     'T1_RU_JINR_Disk', 'T2_FI_HIP', 'T2_UK_London_IC', 'T2_FR_GRIF_LLR',
                                     'T2_US_Nebraska', 'T2_IT_Bari', 'T2_FR_IPHC', 'T2_DE_DESY', 'T2_BE_IIHE',
                                     'T1_DE_KIT_Disk', 'T2_BR_SPRACE', 'T1_US_FNAL_Disk', 'T1_UK_RAL_Disk',
                                     'T2_US_Vanderbilt'])

        # matches the same rules as above (all multi RSEs), but returns an intersection of the RSEs
        resp = prodRucio.getDataLockedAndAvailable(name=DSET2, account="transfer_ops", grouping="ALL")
        # FIXME: there is a serious block distribution problem with Rucio, there should be 6 disk RSE locations..
        self.assertItemsEqual(resp, ['T1_US_FNAL_Disk'])

        # there are no rules for the blocks, but 6 copies for the container level
        resp = prodRucio.getDataLockedAndAvailable(name=BLOCK2, account="transfer_ops", grouping="ALL")
        self.assertItemsEqual(resp, ['T1_IT_CNAF_Disk', 'T1_RU_JINR_Disk', 'T1_UK_RAL_Disk', 'T1_US_FNAL_Disk',
                                     'T2_IT_Bari', 'T2_US_Vanderbilt'])

        # return the  6 copies for the container level, plus the tape one(s)
        resp = prodRucio.getDataLockedAndAvailable(name=BLOCK2, account="transfer_ops",
                                                   grouping="ALL", returnTape=True)
        self.assertItemsEqual(resp, ['T1_IT_CNAF_Disk', 'T1_RU_JINR_Disk', 'T1_UK_RAL_Disk', 'T1_US_FNAL_Disk',
                                     'T2_IT_Bari', 'T2_US_Vanderbilt', 'T1_IT_CNAF_Tape'])

        # there are no rules for the blocks, but 6 copies for the container level
        resp = prodRucio.getDataLockedAndAvailable(name=BLOCK2, account="transfer_ops", grouping="DATASET")
        self.assertItemsEqual(resp, [])
