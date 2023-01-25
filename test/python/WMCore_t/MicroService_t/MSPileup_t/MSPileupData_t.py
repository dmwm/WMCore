"""
File       : MSPileupData.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit tests for MicorService/MSPileup/MSPileupData.py module
"""

# system modules
import time
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData, stripKeys
from Utils.Timers import encodeTimestamp, decodeTimestamp


class MSPileupTest(unittest.TestCase):
    """Unit test for MSPileupData module"""

    def setUp(self):
        """setup unit test class"""
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

    def testStripKeys(self):
        """
        Unit test to test stripKeys helper function
        """
        skeys = ['_id']
        expect = {'pileupId': 1}
        pdict = {'pileupId': 1, '_id': 1}
        pdict = stripKeys(pdict, skeys)
        self.assertDictEqual(pdict, expect)

        pdict = {'pileupId': 1, '_id': 1}
        results = [pdict]
        results = stripKeys(results, skeys)
        self.assertDictEqual(pdict, expect)

    def testTimestampsSerialization(self):
        """
        Unit test to test serialization of timestamps
        """
        tkeys = ['insertTime', 'lastUpdateTime']
        doc = {'pileupId': 1}
        now = int(time.time())
        gnow = time.gmtime(now)
        expect = time.strftime("%Y-%m-%dT%H:%M:%SZ", gnow)
        for key in tkeys:
            doc.update({key: now})
        # encode time stamps
        for key in ['insertTime', 'lastUpdateTime']:
            doc[key] = encodeTimestamp(doc[key])
            self.assertEqual(doc[key], expect)
        # decode time stamps
        for key in ['insertTime', 'lastUpdateTime']:
            doc[key] = decodeTimestamp(doc[key])
            self.assertEqual(int(doc[key]), now)

    def testDocs(self):
        """
        Unit test to test document creation and storage in MongoDB
        """
        skeys = ['_id']
        pname = '/skldjflksdjf/skldfjslkdjf/PREMIX'
        now = int(time.mktime(time.gmtime()))
        expectedRSEs = []
        fullReplicas = 1
        pileupSize = 1
        ruleList = []
        campaigns = []
        containerFraction = 0.0
        replicationGrouping = "ALL"

        pdict = {
            'pileupName': pname,
            'pileupType': 'classic',
            'insertTime': now,
            'lastUpdateTime': now,
            'expectedRSEs': expectedRSEs,
            'currentRSEs': expectedRSEs,
            'fullReplicas': fullReplicas,
            'campaigns': campaigns,
            'containerFraction': containerFraction,
            'replicationGrouping': replicationGrouping,
            'activatedOn': now,
            'deactivatedOn': now,
            'active': True,
            'pileupSize': pileupSize,
            'ruleList': ruleList}

        out = self.mgr.createPileup(pdict)
        self.assertEqual(len(out), 0)

        spec = {'pileupName': pname}
        results = self.mgr.getPileup(spec)
        doc = results[0]
        doc = stripKeys(doc, skeys)
        self.assertDictEqual(pdict, doc)

        doc.update({'pileupSize': 2})
        out = self.mgr.updatePileup(doc)
        self.assertEqual(len(out), 0)
        results = self.mgr.getPileup(spec)
        doc = results[0]
        doc = stripKeys(doc, skeys)
        self.assertEqual(doc['pileupSize'], 2)

        self.mgr.deletePileup(spec)
        self.assertEqual(len(out), 0)
        results = self.mgr.getPileup(spec)
        self.assertEqual(len(results), 0)


if __name__ == '__main__':
    unittest.main()
