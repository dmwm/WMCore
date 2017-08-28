"""
Unit tests for Unified/SiteInfo.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

# system modules
import unittest

# WMCore modules
# from WMCore.Services.MicroService.Unified.Common import elapsedTime
from WMCore.Services.MicroService.Unified.SiteInfo import getNodes, SiteInfo, siteCache


class SiteInfoTest(unittest.TestCase):
    "Unit test for SiteInfo module"
    def setUp(self):
        self.siteInfo = SiteInfo()

    def testGetNodes(self):
        "Test function for getNodes()"
        nodes = getNodes('MSS')
        self.assertEqual(True, 'T1_US_FNAL_MSS' in nodes)

    def testCe2SE(self):
        "Test function for ce2SE()"
        self.assertEqual('T1_US_FNAL_MSS_Disk', self.siteInfo.ce2SE('T1_US_FNAL_MSS'))

    def testSe2CE(self):
        "Test function for se2CE()"
        self.assertEqual('T1_US_FNAL', self.siteInfo.se2CE('T1_US_FNAL_MSS'))
        self.assertEqual('T1_US_FNAL', self.siteInfo.se2CE('T1_US_FNAL_Disk'))

    def testSiteCache(self):
        "Test function for siteCache"
        keys = ['gwmsmon_prod_site_summary', 'gwmsmon_site_summary', 
                'gwmsmon_totals', 'gwmsmon_prod_maxused',
                'mcore', 'detox_sites', 'mss_usage', 'site_queues']
        ssbids = ['106', '107', '108', '109', '136', '158', '159', '160', '237']
        sids = ['1', '2', 'm1', 'm3', 'm4', 'm5', 'm6']
        keys += ['stuck_%s' % k for k in sids]
        keys += ['ssb_%s' % k for k in ssbids]
        self.assertEqual(sorted(keys), sorted(siteCache.siteInfo.keys()))

if __name__ == '__main__':
    unittest.main()
