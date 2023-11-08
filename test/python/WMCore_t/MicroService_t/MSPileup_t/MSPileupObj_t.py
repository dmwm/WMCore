"""
File       : MSPileupObj_t.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Unit tests for MicorService/MSPileup/DataStructs/MSPileupObj.py module
"""

# system modules
import unittest

# WMCore modules
from WMCore.MicroService.MSPileup.DataStructs.MSPileupObj import MSPileupObj
from WMCore.MicroService.Tools.Common import getMSLogger


class MSPileupObjTest(unittest.TestCase):
    """Unit test for MSPileupObj module"""

    def setUp(self):
        """
        setup Unit tests
        """
        self.logger = getMSLogger(False)

    def testMSPileupObj(self):
        """
        Unit test for correct MSPileupObj data
        """
        expectedRSEs = ['rse1', 'rse2']
        fullReplicas = 0
        campaigns = ['c1', 'c2']
        data = {
            'pileupName': '/lksjdfkls/lkjaslkdjlas/PREFIX',
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
        obj = MSPileupObj(data, validRSEs=expectedRSEs)
        for key in ['insertTime', 'lastUpdateTime', 'activatedOn', 'deactivatedOn']:
            self.assertNotEqual(obj.data[key], 0)
        self.assertEqual(obj.data['expectedRSEs'], expectedRSEs)
        self.assertEqual(obj.data['fullReplicas'], fullReplicas)
        self.assertEqual(obj.data['campaigns'], campaigns)

        # check validation functions
        self.assertEqual(obj.validateRSEs(['bla']), False)
        self.assertEqual(obj.validateRSEs(expectedRSEs), True)

        # let's turn off RSEs and check that we'll get exception while
        # creating MSPileupObj
        wrongData = dict(data)
        wrongData['expectedRSEs'] = []
        try:
            MSPileupObj(wrongData, validRSEs=expectedRSEs)
        except Exception as exp:
            self.logger.warning("expected exception %s", str(exp))

        # let's use non complaint pileupName
        wrongData = dict(data)
        wrongData['pileupName'] = 'pileupName'
        try:
            MSPileupObj(wrongData, validRSEs=expectedRSEs)
        except Exception as exp:
            self.logger.warning("expected exception %s", str(exp))

        # change replicationGrouping to bizzare value
        wrongData = dict(data)
        wrongData['replicationGrouping'] = 'bla'
        try:
            MSPileupObj(wrongData, validRSEs=expectedRSEs)
        except Exception as exp:
            self.logger.warning("expected exception %s", str(exp))

    def testWrongMSPileupObj(self):
        """
        Unit test for incorrect MSPileupObj data
        """
        data = {
            'pileupName': 'pileupName',
            'pileupType': 1,
            'expectedRSEs': 'expectedRSEs',
            'fullReplicas': 'fullReplicas',
            'campaigns': 'campaigns',
            'containerFraction': 0,
            'replicationGrouping': "",
            'active': True,
            'pileupSize': 0,
            'ruleIds': []}
        try:
            MSPileupObj(data)
            self.assertIsNone(1, msg="MSPileupObj should not be created")
        except Exception as exp:
            self.logger.warning("expected exception %s", str(exp))


if __name__ == '__main__':
    unittest.main()
