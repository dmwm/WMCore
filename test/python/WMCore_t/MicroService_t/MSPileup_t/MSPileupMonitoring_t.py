"""
Unit tests for MSPileupMonitoring.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.MSPileupMonitoring import flatDocuments, flatKey


class MSPileupMonitoringTest(unittest.TestCase):
    "Unit test for MSPileupMonitoring module"

    def setUp(self):
        rses = ['rse1', 'rse2']
        campaigns = ['c1', 'c2']
        ruleIds = ['1', '2']
        self.doc = {
            'pileupName': '/klsjdfklsd/klsjdflksdj/PREMIX',
            'pileupType': 'classic',
            'expectedRSEs': rses,
            'currentRSEs': rses,
            'fullReplicas': 1,
            'campaigns': campaigns,
            'containerFraction': 1.0,
            'replicationGrouping': "ALL",
            'active': True,
            'pileupSize': 0,
            'ruleIds': ruleIds}

    def testFlatKey(self):
        "test flatKey functions"
        doc = dict(self.doc)
        docs = list(flatKey(doc, 'campaigns'))
        self.assertEqual(len(docs), 2)
        key = 'campaigns'
        nkey = key[:-1]  # new single key, e.g. campaigns -> campaign
        self.assertEqual(key in docs[0], False)
        self.assertEqual(docs[0][nkey], self.doc[key][0])

    def testFlatDocuments(self):
        "test flatDocuments function"
        doc = dict(self.doc)
        docs = list(flatDocuments(doc))
        self.assertEqual(len(docs), 8)
        listKeys = ['campaigns', 'currentRSEs', 'expectedRSEs']
        for doc in docs:
            for key in listKeys:
                vals = self.doc[key]  # original key vaules
                nkey = key[:-1]  # key without s, e.g. campaigns -> campaign
                self.assertEqual(key in doc, False)  # original key should be gone
                val = doc[nkey]  # new value for single key
                self.assertEqual(val in vals, True)


if __name__ == '__main__':
    unittest.main()
