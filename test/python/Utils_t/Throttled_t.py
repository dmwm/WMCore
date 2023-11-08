#!/usr/bin/env python
# pylint: disable-msg=C0103
"""
Unittests for Utilities functions
"""


import time
import threading
import unittest
from Utils.Throttled import UserThrottle, UserThrottleTime
from cherrypy import HTTPError



class UtilitiesTests(unittest.TestCase):
    """
    unittest for Utilities functions
    """
    def setUp(self):
        self.limit = 100
        self.thr = UserThrottleTime(limit=self.limit)
        self.thrc = UserThrottle(limit=3)
        self.thr3 = UserThrottleTime(limit=3)
        self.thr10 = UserThrottleTime(limit=10)

    def testthrottledTimes(self):
        """
        Test that throttled function behave slower than normal
        """
        @self.thr.make_throttled()
        def throttled():
            "Test function for throttled"
            return time.time()
        thr_times = []
        xxx_times = []
        for _ in range(self.limit-1):
            thr_times.append(throttled())
        for _ in range(self.limit-1):
            xxx_times.append(time.time())
        thr_range = max(thr_times) - min(thr_times)
        nor_range = max(xxx_times) - min(xxx_times)
        self.assertEqual(thr_range > nor_range, True)
        self.assertEqual((thr_range - nor_range) > nor_range, True)

    def testthrottledTimeRange(self):
        """
        Test that throttled within certain time range
        """

        @self.thr10.make_throttled()
        def throttled():
            "Test function for throttled"
            return time.time()

        @self.thr3.make_throttled(trange=2)
        def throttled2():
            "Test function for throttled"
            return time.time()

        def test(dim):
            for _ in range(dim):
                throttled2()
        self.assertRaises(HTTPError, test, 5)
        # once assertion happens user will be allowed to make N-additional
        # calls based on elapsed time of throttler
        time.sleep(1)
        throttled2()

        # here we should be fine because throttled function decorated with
        # limit=10
        for _ in range(5):
            throttled()

        # sleep 2 sec (our trange) and use again our throttled2 function
        # this time it should be fine
        time.sleep(2)
        throttled2()
        # but we we'll call it multiple times we'll get back exception
        self.assertRaises(HTTPError, test, 5)

    def testthrottledCounter(self):
        """
        Test that throttled counter version
        """

        @self.thrc.make_throttled()
        def throttled():
            "Test function for throttled"
            return time.time()

        def test(dim):
            for _ in range(dim):
                throttled()
            self.assertRaises(HTTPError, testCounter, 5)

        def testCounter(iterations):
            threads = []
            for _ in range(iterations):
                thr = threading.Thread(target=test, args=(50,))
                thr.start()
                threads.append(thr)
            for thr in threads:
                thr.join()


if __name__ == '__main__':
    unittest.main()
