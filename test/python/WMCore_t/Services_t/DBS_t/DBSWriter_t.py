#!/usr/bin/env python
"""
_DBSWriter_t_

Unit test for the DBS Writer class.
"""

import unittest

from WMCore.Services.DBS.DBS3Writer import DBS3Writer as DBS3Writer
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

# A dummy dataset to play with
DUMMY_DATASET = '/DYToEE_M-50_NNPDF31_TuneCP5_14TeV-powheg-pythia8/Run3Summer19DRPremix-BACKFILL_2024Scenario_106X_mcRun3_2024_realistic_v4-v6/GEN-SIM-RAW'


class DBSWriterTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the API to point at the test server.
        """

        self.dbsReaderUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader/'
        self.dbsWriterUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSWriter/'
        self.dbsReader = None
        self.dbsWriter = None
        super(DBSWriterTest, self).setUp()
        return

    def tearDown(self):
        """
        _tearDown_

        """

        super(DBSWriterTest, self).tearDown()
        return

    def testSetDBSStatus(self):
        self.dbsWriter = DBS3Writer(url=self.dbsReaderUrl,
                                    writeUrl=self.dbsWriterUrl)

        res = self.dbsWriter.setDBSStatus(DUMMY_DATASET, "PRODUCTION")
        self.assertTrue(res)
        res = self.dbsWriter.setDBSStatus(DUMMY_DATASET, "VALID")
        self.assertTrue(res)
        res = self.dbsWriter.setDBSStatus(DUMMY_DATASET, "INVALID")
        self.assertTrue(res)


if __name__ == '__main__':
    unittest.main()
