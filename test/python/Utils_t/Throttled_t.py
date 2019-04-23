#!/usr/bin/env python
# pylint: disable-msg=C0103
"""
Unittests for Utilities functions
"""

from __future__ import division, print_function
import time
import unittest
from Utils.Throttled import global_user_throttle


@global_user_throttle.make_throttled()
def throttled():
    "Test function for throttled"
    return time.time()


class UtilitiesTests(unittest.TestCase):
    """
    unittest for Utilities functions
    """

    def testthrottledTimes(self):
        """
        Test that throttled function behave slower than normal
        """
        rdim = 1000
        thr_times = []
        xxx_times = []
        for _ in range(rdim):
            thr_times.append(throttled())
        for _ in range(rdim):
            xxx_times.append(time.time())
        thr_range = max(thr_times) - min(thr_times)
        nor_range = max(xxx_times) - min(xxx_times)
        print("throttled times: %s" % thr_range)
        print("normal    times: %s" % nor_range)
        self.assertEqual(thr_range > nor_range, True)
        self.assertEqual((thr_range - nor_range) > nor_range, True)

if __name__ == '__main__':
    unittest.main()
