#!/usr/bin/env python
"""
_Timestamps_t_

Tests timestamps metrics
"""

import unittest

from WMCore.Configuration import Configuration
from Utils.Timestamps import addTimestampMetrics


class TimestampsTest(unittest.TestCase):
    """
    Timestamps class provides unit tests for Timestamps module
    """
    def testAddTimestamps(self):
        """
        test timestamps metrics in configuration
        """
        config = Configuration()
        wmJobStart = 1
        wmJobEnd = 2
        addTimestampMetrics(config, wmJobStart, wmJobEnd)
        self.assertEqual(config.WMTiming.WMJobStart, wmJobStart)
        self.assertEqual(config.WMTiming.WMJobEnd, wmJobEnd)
        self.assertEqual(config.WMTiming.WMTotalWallClockTime, wmJobEnd - wmJobStart)


if __name__ == "__main__":
    unittest.main()
