#!/usr/bin/env python
"""
_DBSWriter_t_

Unit test for the DBS Writer class.
"""

import unittest

from WMCore.Services.DBS.DBS3Reader import DBS3Reader as DBSReader
from WMCore.Services.DBS.DBS3Writer import DBS3Writer as DBS3Writer
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

# A dummy dataset to play with
DUMMY_DATASET = '/RelValDarkSUSY_14TeV/DMWM_Test-TC_LHE_PFN_Mar2021_Val_Alanv5-v11/GEN-SIM'

class DBSWriterTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the API to point at the test server.
        """

        self.dbsReaderUrl = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/'
        self.dbsWriterUrl = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSWriter/'
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
        self.dbsReader = DBSReader(self.dbsReaderUrl)
        self.dbsWriter = DBS3Writer(self.dbsWriterUrl)

        res = self.dbsWriter.setDBSStatus(DUMMY_DATASET, "VALID")
        self.assertTrue(res)
        status = self.dbsReader.getDBSStatus(DUMMY_DATASET)
        self.assertEqual(status,"VALID")

        res = self.dbsWriter.setDBSStatus(DUMMY_DATASET, "INVALID")
        self.assertTrue(res)
        status = self.dbsReader.getDBSStatus(DUMMY_DATASET)
        self.assertEqual(status, "INVALID")



if __name__ == '__main__':
    unittest.main()
