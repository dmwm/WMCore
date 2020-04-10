#!/usr/bin/env python
"""
Unit tests for the RucioInjectorPoller module
"""

from __future__ import division

import unittest
from WMComponent.RucioInjector.RucioInjectorPoller import filterDataByTier


class RucioInjectorPollerTest(unittest.TestCase):

    def testFilterDataByTier(self):
        """
        _testFilterDataByTier_

        Test the `filterDataByTier` function, which is supposed to return
        only data for the allowed data tiers.
        """
        allowedTiers = ["NANOAOD", "NANOAODSIM"]
        uninjectedFiles = {"SiteA": {"/dset1/procStr-v1/GEN": ["blah"],
                                     "/dset2/procStr-v1/GEN-SIM": ["blah"],
                                     "/dset3/procStr-v1/AOD": ["blah"],
                                     "/dset4/procStr-v1/NANOAODSIM": ["blah"],
                                     "/dset5/procStr-v1/NANOAOD": ["blah"],
                                     "/dset6/procStr-v1/RECO": ["blah"]},
                           "SiteB": {"/dset7/procStr-v1/NANOAOD": ["blah"]},
                           "SiteC": {"/dset8/procStr-v1/GEN": ["blah"]}}

        finalData = filterDataByTier(uninjectedFiles, allowedTiers)
        self.assertEqual(len(finalData), 3)
        self.assertItemsEqual(finalData.keys(), ["SiteA", "SiteB", "SiteC"])

        self.assertItemsEqual(finalData["SiteA"].keys(), ["/dset4/procStr-v1/NANOAODSIM",
                                                          "/dset5/procStr-v1/NANOAOD"])
        self.assertItemsEqual(finalData["SiteB"].keys(), ["/dset7/procStr-v1/NANOAOD"])
        self.assertItemsEqual(finalData["SiteC"].keys(), [])

        return


if __name__ == '__main__':
    unittest.main()
