#!/usr/bin/env python
"""
Unittests for the Services/MSPileup/MSPileupUtils module
"""
import logging
import unittest

from nose.plugins.attrib import attr

from WMCore.Services.MSPileup.MSPileupUtils import getPileupDocs


class MSPileupUtilsTest(unittest.TestCase):
    """
    Test class for the RucioUtils module
    """

    def setUp(self):
        """
        Setup for unit tests
        """
        self.logger = logging.getLogger()

    def tearDown(self):
        """
        Nothing to be done for this case
        """

    @attr('integration')
    def testGetPileupDocs(self):
        """
        Test getting pileup docs from testbed.
        """
        pdict = {
            "pileupName": "/MinBias_TuneCP5_14TeV-pythia8/PhaseIITDRSpring19GS-106X_upgrade2023_realistic_v2_ext1-v1/GEN-SIM",
            "pileupType": "classic",
            "insertTime": 1680873642,
            "lastUpdateTime": 1706216047,
            "expectedRSEs": [
                "T1_US_FNAL_Disk",
                "T2_CH_CERN"
                ],
            "currentRSEs": [
                "T1_US_FNAL_Disk",
                "T2_CH_CERN"
                ],
            "fullReplicas": 1,
            "campaigns": [
                "Apr2023_Val"
                ],
            "containerFraction": 1.0,
            "replicationGrouping": "ALL",
            "activatedOn": 1706216047,
            "deactivatedOn": 1680873642,
            "active": True,
            "pileupSize": 1233099715874,
            "ruleIds": [
                "55e5a21aecb5445c8aa40581a7bf18d2",
                "67a3fa7252f54507ba1c45f271beb754"
                ],
            "customName": "",
            "transition": []
            }

        dataItem = '/MinBias_TuneCP5_14TeV-pythia8/PhaseIITDRSpring19GS-106X_upgrade2023_realistic_v2_ext1-v1/GEN-SIM'
        msPileupUrl = 'https://cmsweb-testbed.cern.ch/ms-pileup/data/pileup'

        # test without filter
        queryDict = {'query': {'pileupName': dataItem}}
        result = getPileupDocs(msPileupUrl, queryDict)

        self.logger.info('GetPileupDocs Result: %s', result)

        self.assertGreater(len(result), 0)

        # make sure all keys are in response
        for k in pdict:
            self.assertIn(k, result[0])

        # test with a filter
        filterKeys = ['currentRSEs', 'pileupName', 'containerFraction', 'ruleIds']
        queryDict = {'query': {'pileupName': dataItem},
                     'filters': filterKeys}
        result = getPileupDocs(msPileupUrl, queryDict)

        self.assertGreater(len(result), 0)

        self.logger.info('GetPileupDocs Result: %s', result)

        # make sure only filterKeys keys are in response
        for k in pdict:
            if k in filterKeys:
                self.assertIn(k, result[0])
            else:
                self.assertNotIn(k, result[0])


if __name__ == '__main__':
    unittest.main()
