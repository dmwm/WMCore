"""
Timers_t module provide unit tests for Timer module
Unittests for Utilities functions
"""

# system modules
import unittest

# WMCore modules
from Utils.Timers import gmtimeSeconds, encodeTimestamp, decodeTimestamp


class TimersTests(unittest.TestCase):
    """
    unittest for Timers functions
    """
    def testTimingFunctions(self):
        "Test timing functions"
        gmtime = gmtimeSeconds()
        self.assertEqual(gmtime > 0, True)
        self.assertEqual(len("{}".format(gmtime)), 10)

        tst = 1674477244
        expect = '2023-01-23T12:34:04Z'
        val = encodeTimestamp(tst)
        self.assertEqual(val, expect)
        val = decodeTimestamp(val)
        self.assertEqual(val, tst)


if __name__ == '__main__':
    unittest.main()
