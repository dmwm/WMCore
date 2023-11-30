"""
File       : MSPileupTasks_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit tests for MicorService/MSPileup/MSPileupTasks.py module
"""

# system modules
import os
import logging
import unittest

# rucio modules
from rucio.client import Client

# WMCore modules
from WMQuality.Emulators.RucioClient.MockRucioApi import MockRucioApi
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMCore.MicroService.MSPileup.MSPileupTasks import MSPileupTasks
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData
from WMCore.MicroService.MSPileup.MSPileupMonitoring import MSPileupMonitoring
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.Services.Rucio.Rucio import Rucio


class TestRucioClient(Client):
    """Fake implementation for Rucio client"""
    def __init__(self, account='test', logger=None, state='OK'):
        self.account = 'test'
        if logger:
            self.logger = logger
        else:
            self.logger = getMSLogger(False)
        self.rses = ['rse1', 'rse2', 'T2_XX_CERN']
        self.state = state
        self.doc = {'id': '123', 'rse_expression': 'T2_XX_CERN', 'state': self.state}

    def list_replication_rules(self, kwargs):
        """Immitate list_replication_rules Rucio client API"""
        # we will mock rucio doc based on provided state
        # the states are: OK, REPLICATING, STUCK, SUSPENDED, WAITING_APPROVAL, INJECT
        doc = dict(self.doc)
        if self.state != 'OK':
            doc['locks_ok_cnt'] = 2
            doc['locks_replicating_cnt'] = 2
            doc['locks_stuck_cnt'] = 2
        for key, val in kwargs.items():
            doc[key] = val
        docs = [doc]
        self.logger.info("### TestRucioClient docs %s", docs)
        if 'Exception' in self.state:
            raise Exception(self.state)
        return docs

    def delete_replication_rule(self, rid):
        """Immidate delete replication_rule Rucio client API"""
        msg = f"delete rule ID {rid}"
        self.logger.info(msg)

    def add_replication_rule(self, dids, copies, rses, **kwargs):
        """Immitate add replication rule Rucio client API"""
        msg = f"add replication rule for {dids}, copies {copies} rses {rses}"
        self.logger.info(msg)

    def list_rses(self, rseExpression):
        """Immitate get list_rses Rucio client API"""
        for rse in self.rses:
            yield {'rse': rse}

    def get_rse_usage(self, rse):
        """Immitate get rse usage Rucio client API"""
        # it is a generator which provides information about given RSE
        doc = {'id': '123', 'source': 'unavailable',
               'used': 440245583794751,
               'free': None,
               'total': 440245583794751, 'files': 138519,
               'rse': rse}
        yield doc

    def listDataRules(self, pname, **kwargs):
        """Mock the Rucio wrapper listDataRules API"""
        self.logger.info("%s: mocking listDataRules.", self.__class__.__name__)
        return self.list_replication_rules(kwargs)

    def evaluateRSEExpression(self, expr):
        """Mock the Rucio wrapper evaluateRSEExpression API"""
        self.logger.info("%s: mocking evaluateRSEExpression.", self.__class__.__name__)
        return self.rses


class MSPileupTasksTest(EmulatedUnitTestCase):
    """Unit test for MSPileupTasks module"""

    def setUp(self):
        """
        setup Unit tests
        """
        # we will define log stream to capture everything that goes to the log stream
        self.logger = logging.getLogger()

        # setup rucio client
        self.rucioAccount = 'ms-pileup'
        self.hostUrl = 'http://cms-rucio.cern.ch'
        self.authUrl = 'https://cms-rucio-auth.cern.ch'
        creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                 "client_key": os.getenv("X509_USER_KEY", "Unknown")}
        configDict = {'rucio_host': self.hostUrl, 'auth_host': self.authUrl,
                      'creds': creds, 'auth_type': 'x509'}

        # setup rucio wrapper
        testRucioClient = TestRucioClient(logger=self.logger, state='OK')
        self.rucioClient = Rucio(self.rucioAccount, configDict=configDict, client=testRucioClient)

        self.validRSEs = ['rse1', 'rse2']

        # setup pileup data manager
        msConfig = {'reqmgr2Url': 'http://localhost',
                    'rucioAccount': 'wmcore_mspileup',
                    'rucioUrl': 'http://cms-rucio-int.cern.ch',
                    'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                    'mongoDB': 'msPileupDB',
                    'mongoDBCollection': 'msPileupDBCollection',
                    'mongoDBServer': 'mongodb://localhost',
                    'mongoDBReplicaSet': '',
                    'mongoDBUser': None,
                    'mongoDBPassword': None,
                    'mockMongoDB': True}
        self.mgr = MSPileupData(msConfig, skipRucio=True)
        self.monMgr = MSPileupMonitoring(msConfig)

        # setup our pileup data
        self.pname = '/primary/processed/PREMIX'
        pname = self.pname
        fullReplicas = 3
        campaigns = ['c1', 'c2']
        data = {
            'pileupName': pname,
            'pileupType': 'classic',
            'expectedRSEs': self.validRSEs,
            'currentRSEs': self.validRSEs,
            'fullReplicas': fullReplicas,
            'campaigns': campaigns,
            'containerFraction': 1.0,
            'replicationGrouping': "ALL",
            'active': True,
            'pileupSize': 0,
            'ruleIds': ['rse1']}
        self.data = data

        self.mgr.createPileup(data, self.validRSEs)

        # add more docs similar in nature but with different size
        data['pileupName'] = pname.replace('processed', 'processed-2')
        self.mgr.createPileup(data, self.validRSEs)
        data['pileupName'] = pname.replace('processed', 'processed-3')
        self.mgr.createPileup(data, self.validRSEs)

    def testMSPileupTasks(self):
        """
        Unit test for MSPileupTasks
        """
        self.logger.info("---------- CHECK for state=OK -----------")

        obj = MSPileupTasks(self.mgr, self.monMgr, self.logger, self.rucioAccount, self.rucioClient)
        obj.monitoringTask()

        # we added three pileup documents and should have update at least one of them
        # in our report, so we check for update pileup message in report
        report = obj.getReport()
        found = False
        for doc in report.getDocuments():
            if 'update pileup' in doc['entry']:
                found = True
        self.assertEqual(found, True)

        # at this step the T2_XX_CERN should be added to currentRSEs as it is provided
        # by TestRucioClient class via list_replication_rules
        spec = {'pileupName': self.pname}
        results = self.mgr.getPileup(spec)
        self.assertEqual(len(results), 1)
        doc = results[0]
        self.assertEqual('T2_XX_CERN' in doc['currentRSEs'], True)
        obj.activeTask()
        obj.inactiveTask()

        # get report documents and log them accordingly
        report = obj.getReport()
        for uuid, entries in report.getReportByUuid().items():
            msg = f"-------- task {uuid} --------"
            self.logger.info(msg)
            for item in entries:
                self.logger.info(item)

        # now we can test non OK state in Rucio
        self.logger.info("---------- CHECK for state=STUCK -----------")
        self.rucioClient = TestRucioClient(logger=self.logger, state='STUCK')
        obj = MSPileupTasks(self.mgr, self.monMgr, self.logger, self.rucioAccount, self.rucioClient)
        obj.monitoringTask()
        # at this step the T2_XX_CERN should NOT be added to currentRSEs
        spec = {'pileupName': self.pname}
        results = self.mgr.getPileup(spec)
        self.assertEqual(len(results), 1)
        doc = results[0]
        self.assertEqual('T2_XX_CERN' in doc['currentRSEs'], False)
        obj.activeTask()
        obj.inactiveTask()

        # now we can test how our code will behave with rucio exceptions
        self.logger.info("---------- CHECK for state=CustomException -----------")

        # we use CustomException for state to check how our code will
        # handle Rucio API exceptions
        self.rucioClient = TestRucioClient(logger=self.logger, state='CustomException')
        obj = MSPileupTasks(self.mgr, self.monMgr, self.logger, self.rucioAccount, self.rucioClient)
        obj.monitoringTask()

    def testMSPileupTasksWithMockApi(self):
        """
        Unit test for MSPileupTasks with RucioMockApi
        """
        # we may take some mock data from
        # https://github.com/dmwm/WMCore/blob/master/test/data/Mock/RucioMockData.json
        # e.g. /MinimumBias/ComissioningHI-v1/RAW' dataset
        pname = '/MinimumBias/ComissioningHI-v1/RAW'
        data = dict(self.data)
        data['pileupName'] = pname
        self.mgr.createPileup(data, self.validRSEs)

        # now create mock rucio client
        rucioClient = MockRucioApi(self.rucioAccount, hostUrl=self.hostUrl, authUrl=self.authUrl)
        obj = MSPileupTasks(self.mgr, self.monMgr, self.logger, self.rucioAccount, rucioClient)
        obj.monitoringTask()
        obj.activeTask()
        obj.inactiveTask()

        # we added new pileup document and should have update pileup message in report
        report = obj.getReport()
        found = False
        for doc in report.getDocuments():
            if 'update pileup' in doc['entry']:
                found = True
        self.assertEqual(found, True)

        # update doc in MSPileup and call cleanup task to delete it
        data['active'] = False
        data['rulesIds'] = []
        data['currentRSEs'] = []
        data['deactivatedOn'] = 0
        self.mgr.updatePileup(data)
        obj.cleanupTask(0)

    def testMSPileupTasksWithCustomName(self):
        """
        Unit test for MSPileupTasks with RucioMockApi and customName DID
        """
        self.logger.info("---------- testMSPileupTasksWithCustomName ----------")
        # we may take some mock data from
        # https://github.com/dmwm/WMCore/blob/master/test/data/Mock/RucioMockData.json
        # e.g. /Cosmics/ComissioningHI-PromptReco-v1/RECO dataset
        pname = '/Cosmics/ComissioningHI-PromptReco-v1/RECO'
        data = dict(self.data)
        data['pileupName'] = self.pname + 'CUSTOM'
        data['customName'] = pname
        self.mgr.createPileup(data, self.validRSEs)

        # now create mock rucio client
        rucioClient = MockRucioApi(self.rucioAccount, hostUrl=self.hostUrl, authUrl=self.authUrl)
        obj = MSPileupTasks(self.mgr, self.monMgr, self.logger, self.rucioAccount, rucioClient)
        obj.monitoringTask()
        obj.activeTask()
        obj.inactiveTask()
        cmsDict, cusDict = obj.pileupSizeTask()

        # we should have out pname in both cmsDict and cusDict maps
        # the asserts are commented as we do not have customName records in mongoDB yet
        # self.assertEqual(len(cusDict), 1)
        # self.assertTrue(len(cmsDict) > 1)
        # instead we'll only print their numbers such that we can verify it during integration tests
        self.logger.info("pileupSizeTask has cmsDict=%s, cusDict=%s records", len(cmsDict), len(cusDict))

        # we added new pileup document and should have update pileup message in report
        report = obj.getReport()
        found = False
        for doc in report.getDocuments():
            self.logger.info("### doc %s", doc)
            if 'update pileup' in doc['entry']:
                found = True
        self.assertEqual(found, True)


if __name__ == '__main__':
    unittest.main()
