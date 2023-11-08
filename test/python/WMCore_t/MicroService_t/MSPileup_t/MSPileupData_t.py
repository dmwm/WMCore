"""
File       : MSPileupData.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit tests for MicorService/MSPileup/MSPileupData.py module
"""

# system modules
import time
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData, stripKeys, getNewTimestamp
from WMCore.MicroService.MSPileup.MSPileupError import MSPILEUP_SCHEMA_ERROR
from Utils.Timers import encodeTimestamp, decodeTimestamp, gmtimeSeconds


class MSPileupTest(unittest.TestCase):
    """Unit test for MSPileupData module"""

    def setUp(self):
        """setup unit test class"""
        self.validRSEs = ['rse1']
        msConfig = {'reqmgr2Url': 'http://localhost',
                    'rucioAccount': 'wmcore_mspileup',
                    'rucioUrl': 'http://cms-rucio-int.cern.ch',
                    'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                    'validRSEs': self.validRSEs,
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
        expectedRSEs = self.validRSEs
        fullReplicas = 1
        pileupSize = 1
        ruleIds = []
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
            'ruleIds': ruleIds}

        out = self.mgr.createPileup(pdict, self.validRSEs)
        self.assertEqual(len(out), 0)

        # now fail the RSE validation
        out = self.mgr.createPileup(pdict, ['rse2'])[0]
        self.assertEqual(out['error'], "MSPileupError")
        self.assertEqual(out['code'], 7)
        expect = "Failed to create MSPileupObj, MSPileup input is invalid, expectedRSEs value ['rse1'] is not in validRSEs ['rse2']"
        self.assertEqual(out['message'], expect)

        spec = {'pileupName': pname}
        results = self.mgr.getPileup(spec)
        doc = results[0]
        doc = stripKeys(doc, skeys)
        self.assertDictEqual(pdict, doc)

        doc.update({'pileupSize': 2})
        out = self.mgr.updatePileup(doc, self.validRSEs)
        self.assertEqual(len(out), 0)
        results = self.mgr.getPileup(spec)
        doc = results[0]
        doc = stripKeys(doc, skeys)
        self.assertEqual(doc['pileupSize'], 2)

        self.mgr.deletePileup(spec)
        self.assertEqual(len(out), 0)
        results = self.mgr.getPileup(spec)
        self.assertEqual(len(results), 0)

    def testMSPileupQuery(self):
        "test MSPileup query API"
        pname = '/skldjflksdjf/skldfjslkdjf/PREMIX'
        spec = {"pileupName": pname}
        res = self.mgr.getPileup(spec)
        self.assertEqual(len(res), 0)

        spec = {"bla": 1}
        res = self.mgr.sanitizeQuery(spec)
        # validate should return list of single dictionary
        # [{'data': {'bla': 1}, 'message': 'schema error', 'code': 7, 'error': 'MSPileupError'}]
        self.assertEqual(len(res), 1)
        err = res[0]
        self.assertEqual(err.get('error'), 'MSPileupError')
        self.assertEqual(err.get('code'), MSPILEUP_SCHEMA_ERROR)

        # and we should get zero results
        res = self.mgr.getPileup(spec)
        self.assertEqual(len(res), 0)

    def testGetNewTimestamp(self):
        """Test the getNewTimestamp function"""
        timeNow = gmtimeSeconds()
        resp = getNewTimestamp({})
        self.assertEqual(len(resp), 1)
        self.assertTrue(resp['lastUpdateTime'] >= timeNow)

        resp = getNewTimestamp({'lastUpdateTime': 1})
        self.assertEqual(len(resp), 1)
        self.assertTrue(resp['lastUpdateTime'] >= timeNow)

        resp = getNewTimestamp({'active': True})
        self.assertEqual(len(resp), 2)
        self.assertTrue(resp['lastUpdateTime'] >= timeNow)
        self.assertTrue(resp['activatedOn'] >= timeNow)
        self.assertFalse('deactivatedOn' in resp)

        resp = getNewTimestamp({'active': False})
        self.assertEqual(len(resp), 2)
        self.assertTrue(resp['lastUpdateTime'] >= timeNow)
        self.assertTrue(resp['deactivatedOn'] >= timeNow)
        self.assertFalse('activatedOn' in resp)


if __name__ == '__main__':
    unittest.main()
