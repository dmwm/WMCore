#!/usr/bin/env python
"""
Unittests for the Services/MSPileup/MSPileupUtils module
"""
import logging
import unittest

from nose.plugins.attrib import attr

from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMCore.MicroService.MSPileup.DataStructs.MSPileupObj import schema
from WMCore.Services.MSPileup.MSPileupUtils import getPileupDocs


class MSPileupUtilsTest(EmulatedUnitTestCase):
    """
    Test class for the RucioUtils module
    """

    def setUp(self):
        """
        Setup for unit tests
        """
        self.logger = logging.getLogger()
        super().setUp()

    def tearDown(self):
        """
        Nothing to be done for this case
        """

    @attr('integration')
    def testGetPileupDocs(self):
        """
        Test getting pileup docs from testbed.
        """
        pdict = schema()

        dataItem = '/MinBias_TuneCP5_14TeV-pythia8/PhaseIITDRSpring19GS-106X_upgrade2023_realistic_v2_ext1-v1/GEN-SIM'
        msPileupUrl = 'https://cmsweb-testbed.cern.ch/ms-pileup/data/pileup'

        # test without filter
        queryDict = {'query': {'pileupName': dataItem}}
        result = getPileupDocs(msPileupUrl, queryDict, method='POST')

        self.logger.info('GetPileupDocs Result: %s', result)

        self.assertGreater(len(result), 0)

        # make sure all keys are in response
        for k in pdict:
            self.assertIn(k, result[0])

        # test with a filter
        filterKeys = ['currentRSEs', 'pileupName', 'containerFraction', 'ruleIds']
        queryDict = {'query': {'pileupName': dataItem},
                     'filters': filterKeys}
        result = getPileupDocs(msPileupUrl, queryDict, method='POST')

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
