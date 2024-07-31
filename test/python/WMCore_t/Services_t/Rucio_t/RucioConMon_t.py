#!/usr/bin/env python
"""
_RucioConMon_t_

Unit tests for RucioConMon WMCore Service class
"""

import unittest

from WMCore.Services.RucioConMon.RucioConMon import RucioConMon


class RucioConMonTest(unittest.TestCase):
    """
    Unit tests for RucioConMon Service module
    """

    def testGetRSEUnmerged(self):
        """
        Test getRSEUnmerged method using both zipped and unzipped requests
        This test uses specific rse name which can be changed to any other RSE.
        """
#         url = "https://cmsweb.cern.ch/rucioconmon/WM/files?rse=T2_TR_METU&format=raw"
        mgr = RucioConMon("https://cmsweb.cern.ch/rucioconmon/unmerged")
        rseName = "T2_RU_ITEP"
        dataUnzipped = [item for item in mgr.getRSEUnmerged(rseName, zipped=False)]
        dataZipped = [item for item in mgr.getRSEUnmerged(rseName, zipped=True)]
        self.assertTrue(dataUnzipped == dataZipped)


if __name__ == '__main__':
    unittest.main()
