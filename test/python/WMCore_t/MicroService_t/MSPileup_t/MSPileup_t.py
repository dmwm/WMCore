"""
Unit tests for MSPileup.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import cherrypy
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.MSPileup import MSPileup
from WMCore.MicroService.MSPileup.DataStructs.MSPileupObj import MSPileupObj
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class MSPileupTest(EmulatedUnitTestCase):
    "Unit test for MSPileup module"
    def setUp(self):
        """
        set up unit test generic objects
        """
        super(MSPileupTest, self).setUp()
        cherrypy.request.user = "test"
        self.validRSEs = ['rse1', 'rse2']
        msConfig = {'reqmgr2Url': 'http://localhost',
                    'authz_key': '123',
                    'rucioAccount': 'wmcore_mspileup',
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
            'containerFraction': 0.0,
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
        res = self.mgr.updatePileup(doc)
        self.assertEqual(len(res), 0)

        # get doc
        res = self.mgr.getPileup(**self.spec)
        self.assertEqual(res[0]['pileupType'], "premix")

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
