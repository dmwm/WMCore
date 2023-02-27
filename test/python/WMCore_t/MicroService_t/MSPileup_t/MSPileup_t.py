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


class MSPileupTest(unittest.TestCase):
    "Unit test for MSPileup module"
    def setUp(self):
        """
        set up unit test generic objects
        """
        cherrypy.request.user = "test"
        self.validRSEs = ['rse1', 'rse2']
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
                    'validRSEs': self.validRSEs,
                    'mockMongoDB': True}
        self.mgr = MSPileup(msConfig)


        self.pname = '/lksjdflksdjf/kljsdklfjsldfj/PREMIX'
        expectedRSEs = self.validRSEs
        fullReplicas = 0
        campaigns = ['c1', 'c2']
        data = {
            'pileupName': self.pname,
            'pileupType': 'classic',
            'expectedRSEs': expectedRSEs,
            'currentRSEs': expectedRSEs,
            'fullReplicas': fullReplicas,
            'campaigns': campaigns,
            'containerFraction': 0.0,
            'replicationGrouping': "ALL",
            'active': True,
            'pileupSize': 0,
            'ruleList': []}

        obj = MSPileupObj(data, validRSEs=self.validRSEs)
        for key in ['insertTime', 'lastUpdateTime', 'activatedOn', 'deactivatedOn']:
            self.assertNotEqual(obj.data[key], 0)
        self.assertEqual(obj.data['expectedRSEs'], expectedRSEs)
        self.assertEqual(obj.data['fullReplicas'], fullReplicas)
        self.assertEqual(obj.data['campaigns'], campaigns)
        self.doc = obj.getPileupData()

    def testMSPileupHTTPApis(self):
        "test MSPileup HTTP APIs"

        # create doc
        res = self.mgr.createPileup(self.doc)
        self.assertEqual(len(res), 0)

        # get doc
        spec = {'pileupName': self.pname}
        res = self.mgr.getPileup(**spec)
        self.assertEqual(len(res), 1)
        # self.assertDictEqual(res[0], self.doc)

        # query doc
        projection = ["pileupType"]
        res = self.mgr.queryDatabase(spec, projection)
        self.assertEqual(len(res), 1)
        # self.assertEqual(res[0], ["classic"])

        # update doc
        doc = dict(self.doc)
        doc["pileupType"] = "new"
        res = self.mgr.updatePileup(doc)
        self.assertEqual(len(res), 0)

        # query doc
        res = self.mgr.queryDatabase(spec, projection)
        self.assertEqual(len(res), 1)
        # self.assertEqual(res[0], ["new"])

        # delete doc
        res = self.mgr.deletePileup(spec)
        self.assertEqual(len(res), 0)


if __name__ == '__main__':
    unittest.main()
