"""
File       : MSPileupData.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit tests for MicorService/MSPileup/MSPileupData.py module
"""

# system modules
import time
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.MSPileupData import MSPileupData, stripKeys, getNewTimestamp, \
    customDID, addTransitionRecord
from WMCore.MicroService.MSPileup.MSPileupError import MSPILEUP_SCHEMA_ERROR
from WMCore.MicroService.Tools.Common import getMSLogger
from Utils.Timers import encodeTimestamp, decodeTimestamp, gmtimeSeconds


class MSPileupTest(unittest.TestCase):
    """Unit test for MSPileupData module"""

    def setUp(self):
        """setup unit test class"""
        self.logger = getMSLogger(False)
        self.validRSEs = ['rse1']
        msConfig = {'reqmgr2Url': 'http://localhost',
                    'rucioAccount': 'wmcore_test',
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

        pname = '/skldjflksdjf/skldfjslkdjf/PREMIX'
        now = int(time.mktime(time.gmtime()))
        expectedRSEs = self.validRSEs
        fullReplicas = 1
        pileupSize = 1
        ruleIds = []
        campaigns = []
        containerFraction = 1.0
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
            'customName': '',
            'transition': [],
            'ruleIds': ruleIds}
        self.doc = pdict
        self.pname = pname
        self.userDN = 'test-dn'
        self.tranRecord = {'containerFraction': 1.0,
                           'customDID': self.pname,
                           'DN': self.userDN,
                           'updateTime': gmtimeSeconds()}

    def testCustomDID(self):
        """Test the customDID function"""
        pname = "/abc/xyz/MINIAOD"
        did = customDID(pname)
        self.assertTrue(did == (pname + '-V1'))
        did = customDID(did)
        self.assertTrue(did == (pname + '-V2'))

        # test more complex suffix
        pname = "/abc/xyz/MINIAOD-V123"
        did = customDID(pname)
        expect = "/abc/xyz/MINIAOD-V124"
        self.assertTrue(did == expect)

    def testUpdateTransitionRecord(self):
        """Test the addTransitionRecord function"""
        self.logger.info("test addTransitionRecord function")
        doc = dict(self.doc)
        userDN = ''
        doc['transition'] = [self.tranRecord]
        addTransitionRecord(doc, userDN, self.logger)
        self.logger.info("1st test (empty DN) %s", doc)
        self.assertTrue(len(doc['transition']), 1)
        trec = doc['transition'][0]
        self.logger.info("trec %s", trec)
        self.assertTrue(trec['DN'], self.userDN)

        # test when our document contains smaller fraction
        fraction = 0.5
        doc['containerFraction'] = fraction
        doc['transition'] = [self.tranRecord]
        addTransitionRecord(doc, userDN, self.logger)
        self.logger.info("2nd test (container fraction) %s", doc)
        self.assertTrue(len(doc['transition']), 2)
        # check that all transition record have the same DN
        for trec in doc['transition']:
            self.assertTrue(trec['DN'], self.userDN)
        # check container fraction of last record
        trec = doc['transition'][-1]
        self.logger.info("trec %s", trec)
        self.assertTrue(trec['containerFraction'], fraction)


        # test use-case when we supply the same container fraction
        addTransitionRecord(doc, self.userDN, self.logger)
        self.logger.info("2nd test (same container fraction) %s", doc)
        self.assertTrue(len(doc['transition']), 2)

        # test when our document contains smaller fraction and we add new transition record with custom DN
        userDN = 'bla'
        addTransitionRecord(doc, userDN, self.logger)
        self.logger.info("3rd test (custom DN) %s", doc)
        self.assertTrue(len(doc['transition']), 3)
        # check that last transition record will have custom DN
        trec = doc['transition'][-1]
        self.assertTrue(trec['DN'], userDN)
        # and first and second transition record should have original userDN
        trec = doc['transition'][0]
        self.assertTrue(trec['DN'], self.userDN)
        trec = doc['transition'][1]
        self.assertTrue(trec['DN'], self.userDN)

        # test daemon use-case, i.e. when we update pileup record attribute w/o transition
        doc = dict(self.doc)
        doc['transition'] = [self.tranRecord]
        doc['pileupSize'] = 100
        userDN = ''
        addTransitionRecord(doc, userDN, self.logger)
        self.logger.info("4th test (daemon test) %s", doc)
        self.assertTrue(len(doc['transition']), 1)
        trec = doc['transition'][0]
        self.assertTrue(trec['DN'], self.userDN)

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
        pdict = dict(self.doc)
        pname = self.pname
        out = self.mgr.createPileup(pdict, self.validRSEs, userDN=self.userDN)
        self.assertEqual(len(out), 0)

        # now fail the RSE validation
        out = self.mgr.createPileup(pdict, ['rse2'], userDN=self.userDN)[0]
        self.assertEqual(out['error'], "MSPileupError")
        self.assertEqual(out['code'], 7)
        expect = "Failed to create MSPileupObj, MSPileup input is invalid, expectedRSEs value ['rse1'] is not in validRSEs ['rse2']"
        self.assertEqual(out['message'], expect)

        spec = {'pileupName': pname}
        results = self.mgr.getPileup(spec)
        doc = results[0]
        doc = stripKeys(doc, skeys)
        # add transition record to original pdict document because it is added internally in createPileup API
        pdict['transition'] = doc['transition']
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
