"""
Unit tests for MSPileup.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import unittest

import cherrypy

# WMCore modules
from Utils.Timers import gmtimeSeconds
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.MicroService.MSPileup.MSPileup import MSPileup
from WMCore.MicroService.MSPileup.MSPileupData import customDID
from WMCore.MicroService.MSPileup.DataStructs.MSPileupObj import MSPileupObj, schema


class MSPileupTest(EmulatedUnitTestCase):
    "Unit test for MSPileup module"
    def setUp(self):
        """
        set up unit test generic objects
        """
        self.logger = getMSLogger(False)
        super(MSPileupTest, self).setUp()
        cherrypy.request.user = "test"
        self.validRSEs = ['rse1', 'rse2']
        msConfig = {'reqmgr2Url': 'http://localhost',
                    'authz_key': '123',
                    'rucioAccount': 'wmcore_pileup',
                    'rucioUrl': 'http://cms-rucio-int.cern.ch',
                    'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                    'mongoDB': 'msPileupDB',
                    'mongoDBCollection': 'msPileupDBCollection',
                    'mongoDBServer': 'mongodb://localhost',
                    'mongoDBReplicaSet': '',
                    'mongoDBUser': None,
                    'mongoDBPassword': None,
                    'validRSEs': self.validRSEs,
                    'mockMongoDB': True}
        self.mgr = MSPileup(msConfig)

        pname = '/lksjdflksdjf/kljsdklfjsldfj/PREMIX'
        self.spec = {'pileupName': pname}

        expectedRSEs = self.validRSEs
        fullReplicas = 0
        campaigns = ['c1', 'c2']
        data = {
            'pileupName': pname,
            'pileupType': 'classic',
            'expectedRSEs': expectedRSEs,
            'currentRSEs': expectedRSEs,
            'fullReplicas': fullReplicas,
            'campaigns': campaigns,
            'containerFraction': 1.0,
            'replicationGrouping': "ALL",
            'active': True,
            'pileupSize': 0,
            'ruleIds': []}

        obj = MSPileupObj(data, validRSEs=self.validRSEs)
        for key in ['insertTime', 'lastUpdateTime', 'activatedOn', 'deactivatedOn']:
            self.assertNotEqual(obj.data[key], 0)
        self.assertEqual(obj.data['expectedRSEs'], expectedRSEs)
        self.assertEqual(obj.data['fullReplicas'], fullReplicas)
        self.assertEqual(obj.data['campaigns'], campaigns)
        self.doc = obj.getPileupData()

    def createDoc(self):
        """Creates a pileup object in the database"""
        return self.mgr.createPileup(self.doc)

    def testMSPileupStatus(self):
        """Test MSPileup status API"""
        res = self.mgr.status()
        self.assertEqual(res['error'], '')
        self.assertEqual(res['thread_id'], 'MainThread')

    def testMSPileupGet(self):
        """Test MSPileup createPileup and getPileup API"""
        self.assertEqual(len(self.createDoc()), 0)

        # get doc
        res = self.mgr.getPileup(**self.spec)
        self.assertEqual(len(res), 1)

        # get doc with one filter
        spec = self.spec.copy()
        spec['filters'] = ['currentRSEs']
        res = self.mgr.getPileup(**spec)
        self.assertEqual(len(res), 1)
        self.assertEqual(len(res[0].keys()), len(spec['filters']))

        for k in res[0]:
            self.assertIn(k, spec['filters'])

        # get doc with two filters
        spec = self.spec.copy()
        spec['filters'] = ['currentRSEs', 'containerFraction']
        res = self.mgr.getPileup(**spec)
        self.assertEqual(len(res), 1)
        self.assertEqual(len(res[0].keys()), len(spec['filters']))

        for k in res[0]:
            self.assertIn(k, spec['filters'])

        # get doc with empty string filter
        spec = self.spec.copy()
        spec['filters'] = ['']
        res = self.mgr.getPileup(**spec)
        self.assertEqual(len(res), 1)
        self.assertEqual(len(res[0].keys()), len(schema().keys()))

        # get doc with two filters with one empty string
        spec = self.spec.copy()
        spec['filters'] = ['currentRSEs', '']
        res = self.mgr.getPileup(**spec)
        self.assertEqual(len(res), 1)
        self.assertEqual(len(res[0].keys()), len(spec['filters']) - 1)

        for k in res[0]:
            self.assertIn(k, spec['filters'])

    def testMSPileupQuery(self):
        """Test MSPileup createPileup and queryDatabase API"""
        self.assertEqual(len(self.createDoc()), 0)

        # query doc
        projection = ["pileupType"]
        res = self.mgr.queryDatabase(self.spec, projection)
        self.assertEqual(len(res), 1)

    def testMSPileupUpdate(self):
        """Test MSPileup createPileup and updatePileup API"""
        self.assertEqual(len(self.createDoc()), 0)
        self.assertEqual(self.doc['pileupType'], "classic")

        # update doc
        doc = dict(self.doc)
        doc["pileupType"] = "premix"
        # add transition record as it now requires for update API
        customName = customDID(doc['pileupName'])
        trec = {'DN': 'localhost-test', 'containerFraction': 1, 'customDID': customName, 'updateTime': gmtimeSeconds()}
        doc.update({'transition': [trec]})

        res = self.mgr.updatePileup(doc)
        self.assertEqual(len(res), 0)

        # get doc
        res = self.mgr.getPileup(**self.spec)
        self.assertEqual(res[0]['pileupType'], "premix")

    def testMSPileupUpdateTransition(self):
        """Test MSPileup updatePileup API with transition logic"""
        self.assertEqual(len(self.createDoc()), 0)
        self.assertEqual(self.doc['pileupType'], "classic")

        # update doc
        doc = dict(self.doc)
        doc["pileupType"] = "premix"
        # add transition record as it now requires for update API
        # note: for first transition record we do not change custom name
        # see https://gist.github.com/amaltaro/b4f9bafc0b58c10092a0735c635538b5
        customName = doc['pileupName']
        trec = {'DN': 'localhost-test', 'containerFraction': 1, 'customDID': customName, 'updateTime': gmtimeSeconds()}
        doc.update({'transition': [trec]})

        res = self.mgr.updatePileup(doc)
        self.assertEqual(len(res), 0)
        self.logger.info("updatePileup %s", res)

        # get doc
        res = self.mgr.getPileup(**self.spec)
        self.assertEqual(res[0]['pileupType'], "premix")
        self.logger.info("getPileup %s", res)

        # now we'll perform check for transition records according to logic outlined in
        # https://gist.github.com/amaltaro/b4f9bafc0b58c10092a0735c635538b5
        # Please note: we can only change steps 1-5 since there is no MSPileupTasks is involved

        # add new spec with transition change
        spec = {'pileupName': doc['pileupName'], 'containerFraction': 0.5}
        res = self.mgr.updatePileup(spec)
        self.logger.info("updatePileup %s", res)
        self.assertEqual(len(res), 0)

        # get doc
        res = self.mgr.getPileup(**self.spec)
        self.assertEqual(res[0]['pileupType'], "premix")
        self.logger.info("getPileup %s", res)
        record = res[0]
        self.assertEqual(len(record['transition']), 2)
        self.assertEqual(record['customName'], '')

        # check transition records
        for idx, rec in enumerate(record['transition']):
            if idx == 0:
                self.assertEqual(rec['customDID'], doc['pileupName'])
                self.assertEqual(rec['containerFraction'], 1.0)
            elif idx == 1:
                self.assertEqual(rec['customDID'], doc['pileupName'] + '-V1')
                self.assertEqual(rec['containerFraction'], 1.0)

    def testMSPileupDelete(self):
        """Test MSPileup createPileup and deletePileup API"""
        self.assertEqual(len(self.createDoc()), 0)

        # delete doc
        res = self.mgr.deletePileup(self.spec)
        self.assertEqual(len(res), 0)

        # get doc - already deleted
        res = self.mgr.getPileup(**self.spec)
        self.assertEqual(res, [])


if __name__ == '__main__':
    unittest.main()
